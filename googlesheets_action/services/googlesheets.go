package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"googlesheets_action/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

const googleSheetsAPIBase = "https://sheets.googleapis.com/v4/spreadsheets"

type GoogleSheetsService struct {
	httpClient *http.Client
}

func NewGoogleSheetsService() *GoogleSheetsService {
	return &GoogleSheetsService{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// ConfigProvider defines an interface to retrieve an ActionConfig by WorkflowID
type ConfigProvider interface {
	GetConfig(workflowID string) (models.ActionConfig, error)
	UpdateAuth(workflowID string, auth map[string]models.AuthData) error
}

// PublisherProvider defines an interface to publish ActionResults
type PublisherProvider interface {
	Publish(workflowID string, result models.ActionResult) error
}

func getAuth(ctx map[string]models.AuthData) models.AuthData {
	for _, a := range ctx {
		return a
	}
	return models.AuthData{}
}

// templatePattern matches {{trigger.payload.field_name}} patterns.
var templatePattern = regexp.MustCompile(`\{\{trigger\.payload\.(\w+)\}\}`)

// resolveTemplates replaces all {{trigger.payload.X}} templates in the config's
// string fields with corresponding values from the trigger event payload.
func resolveTemplates(cfg *models.ActionConfig, payload map[string]interface{}) {
	if payload == nil || cfg.RawConfig == nil {
		return
	}

	// Deep-copy RawConfig so we don't mutate the original stored config.
	resolved := make(map[string]interface{}, len(cfg.RawConfig))
	for k, v := range cfg.RawConfig {
		resolved[k] = v
	}
	cfg.RawConfig = resolved

	resolveString := func(s string) string {
		if !strings.Contains(s, "{{trigger.payload.") {
			return s
		}
		return templatePattern.ReplaceAllStringFunc(s, func(match string) string {
			submatches := templatePattern.FindStringSubmatch(match)
			if len(submatches) < 2 {
				return match
			}
			key := submatches[1]
			if val, ok := payload[key]; ok {
				return fmt.Sprintf("%v", val)
			}
			log.Printf("[resolveTemplates] WARNING: payload missing key '%s'", key)
			return match
		})
	}

	for k, v := range cfg.RawConfig {
		if s, ok := v.(string); ok {
			cfg.RawConfig[k] = resolveString(s)
		}
	}

	// Re-extract typed fields from resolved raw config
	if v, ok := cfg.RawConfig["spreadsheet_id"].(string); ok {
		cfg.SpreadsheetID = v
	}
	if v, ok := cfg.RawConfig["worksheet"].(string); ok {
		cfg.Worksheet = v
	}
	if v, ok := cfg.RawConfig["cell_coordinates"].(string); ok {
		cfg.CellCoordinates = v
	}
	if v, ok := cfg.RawConfig["value"].(string); ok {
		cfg.Value = v
	}
	if v, ok := cfg.RawConfig["row_values"].(string); ok {
		cfg.RowValues = v
	}
}

// GetValidAuth checks if the token is expired and refreshes it if necessary.
func (s *GoogleSheetsService) GetValidAuth(workflowID string, cfgProvider ConfigProvider) (models.AuthData, error) {
	cfg, err := cfgProvider.GetConfig(workflowID)
	if err != nil {
		return models.AuthData{}, err
	}

	auth := getAuth(cfg.AuthContext)
	if auth.AccessToken == "" {
		return models.AuthData{}, fmt.Errorf("no auth data found")
	}

	// Check if token is expired or about to expire in 5 minutes
	if !auth.Expiry.IsZero() && time.Now().Add(5*time.Minute).After(auth.Expiry) {
		log.Printf("[GoogleSheetsService] Token expired or expiring soon for workflow %s, refreshing...", workflowID)
		newAuth, err := s.RefreshAccessToken(auth)
		if err != nil {
			return models.AuthData{}, fmt.Errorf("refresh failed: %w", err)
		}

		// Update the cache
		for k := range cfg.AuthContext {
			cfg.AuthContext[k] = newAuth
			break
		}
		if err := cfgProvider.UpdateAuth(workflowID, cfg.AuthContext); err != nil {
			log.Printf("[GoogleSheetsService] Warning: failed to update auth cache: %v", err)
		}
		return newAuth, nil
	}

	return auth, nil
}

func (s *GoogleSheetsService) RefreshAccessToken(auth models.AuthData) (models.AuthData, error) {
	tokenURL := "https://oauth2.googleapis.com/token"
	clientID := os.Getenv("GOOGLE_CLIENT_ID")
	clientSecret := os.Getenv("GOOGLE_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		return models.AuthData{}, fmt.Errorf("GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET not set")
	}

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", auth.RefreshToken)
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)

	req, err := http.NewRequest("POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return models.AuthData{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return models.AuthData{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.AuthData{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return models.AuthData{}, fmt.Errorf("refresh failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
		Scope        string `json:"scope"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return models.AuthData{}, err
	}

	// Update auth fields
	auth.AccessToken = tokenResp.AccessToken
	if tokenResp.RefreshToken != "" {
		auth.RefreshToken = tokenResp.RefreshToken
	}
	auth.Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)

	log.Printf("[GoogleSheetsService] Token refreshed successfully. New expiry: %v", auth.Expiry)
	return auth, nil
}

func publishResult(publisher PublisherProvider, task models.ActionTask, resultOutput map[string]interface{}, elapsedMs int64, procErr error) {
	if publisher == nil {
		return
	}
	actionResult := models.ActionResult{
		TaskID:       task.ID,
		WorkflowID:   task.WorkflowID,
		Timestamp:    time.Now().UTC(),
		ResponseTime: elapsedMs,
	}

	if procErr != nil {
		actionResult.Success = false
		actionResult.Status = "error"
		actionResult.Error = procErr.Error()
	} else {
		actionResult.Success = true
		actionResult.Status = "success"
		actionResult.Output = resultOutput
		actionResult.RetryCount = 0
	}

	if pubErr := publisher.Publish(task.WorkflowID, actionResult); pubErr != nil {
		log.Printf("Failed to publish action result: %v", pubErr)
	}
}

// HandleTaskRouter dynamically routes to the specific capability method based on CapabilityKey
func (s *GoogleSheetsService) HandleTaskRouter(cfgProvider ConfigProvider, publisher PublisherProvider) func(d amqp.Delivery) {
	return func(d amqp.Delivery) {
		var task models.ActionTask
		if err := json.Unmarshal(d.Body, &task); err != nil {
			log.Printf("Error unmarshaling task: %v", err)
			d.Nack(false, false)
			return
		}

		cfg, err := cfgProvider.GetConfig(task.WorkflowID)
		if err != nil {
			log.Printf("Error fetching config for workflow %s: %v", task.WorkflowID, err)
			d.Nack(false, false)
			return
		}

		// Resolve {{trigger.payload.X}} templates in config using trigger event payload.
		resolveTemplates(&cfg, task.Payload)

		auth, err := s.GetValidAuth(task.WorkflowID, cfgProvider)
		if err != nil {
			log.Printf("Error ensuring valid auth: %v", err)
			d.Nack(false, false)
			return
		}

		capability := cfg.CapabilityKey

		var procErr error
		var resultOutput map[string]interface{}
		var elapsedMs int64

		switch capability {
		case "googlesheets_update_cell", "googlesheets_update_cell_capability":
			resultOutput, elapsedMs, procErr = s.UpdateCell(auth, cfg)
		case "googlesheets_add_row", "googlesheets_add_row_capability":
			resultOutput, elapsedMs, procErr = s.AddRow(auth, cfg)
		default:
			log.Printf("Unknown capability key: %s", capability)
			d.Nack(false, false)
			return
		}

		// Retry once on 401 Unauthorized if not already refreshed
		if procErr != nil && strings.Contains(procErr.Error(), "401") {
			log.Printf("[GoogleSheetsService] Action failed with 401, trying immediate refresh and retry for workflow %s", task.WorkflowID)
			newAuth, refreshErr := s.RefreshAccessToken(auth)
			if refreshErr == nil {
				// Update cache
				for k := range cfg.AuthContext {
					cfg.AuthContext[k] = newAuth
					break
				}
				_ = cfgProvider.UpdateAuth(task.WorkflowID, cfg.AuthContext)

				// Retry the action
				switch capability {
				case "googlesheets_update_cell", "googlesheets_update_cell_capability":
					resultOutput, elapsedMs, procErr = s.UpdateCell(newAuth, cfg)
				case "googlesheets_add_row", "googlesheets_add_row_capability":
					resultOutput, elapsedMs, procErr = s.AddRow(newAuth, cfg)
				}
			} else {
				log.Printf("[GoogleSheetsService] Refresh failed during retry: %v", refreshErr)
			}
		}

		publishResult(publisher, task, resultOutput, elapsedMs, procErr)

		if procErr != nil {
			log.Printf("Error processing capability %s: %v", capability, procErr)
			d.Nack(false, true)
			return
		}

		d.Ack(false)
	}
}

func (s *GoogleSheetsService) doRequest(method, endpoint string, auth models.AuthData, body []byte) (map[string]interface{}, int64, error) {
	start := time.Now()

	req, err := http.NewRequest(method, endpoint, bytes.NewBuffer(body))
	if err != nil {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("creating http request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		return nil, elapsed, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, elapsed, fmt.Errorf("reading response body: %w", err)
	}

	var result map[string]interface{}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			// fallback if it's not JSON
			result = map[string]interface{}{"raw": string(respBody)}
		}
	} else {
		result = map[string]interface{}{"status": "success"}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errMsg := "API error"
		if errObj, ok := result["error"].(map[string]interface{}); ok {
			if msg, ok := errObj["message"].(string); ok {
				errMsg = msg
			}
		} else if strings.TrimSpace(string(respBody)) != "" {
			errMsg = string(respBody)
		}
		return nil, elapsed, fmt.Errorf("google sheets api returned %d: %s", resp.StatusCode, errMsg)
	}

	return result, elapsed, nil
}

func (s *GoogleSheetsService) UpdateCell(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.SpreadsheetID == "" {
		return nil, 0, fmt.Errorf("spreadsheet_id is required")
	}
	if cfg.CellCoordinates == "" {
		return nil, 0, fmt.Errorf("cell_coordinates is required")
	}

	rangeStr := cfg.CellCoordinates
	if cfg.Worksheet != "" {
		rangeStr = fmt.Sprintf("'%s'!%s", cfg.Worksheet, cfg.CellCoordinates)
	}

	endpoint := fmt.Sprintf("%s/%s/values/%s?valueInputOption=USER_ENTERED",
		googleSheetsAPIBase,
		url.PathEscape(cfg.SpreadsheetID),
		url.PathEscape(rangeStr),
	)

	bodyData := map[string]interface{}{
		"range":          rangeStr,
		"majorDimension": "ROWS",
		"values": [][]interface{}{
			{cfg.Value},
		},
	}
	body, _ := json.Marshal(bodyData)

	return s.doRequest("PUT", endpoint, auth, body)
}

func (s *GoogleSheetsService) AddRow(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.SpreadsheetID == "" {
		return nil, 0, fmt.Errorf("spreadsheet_id is required")
	}

	rangeStr := "A1"
	if cfg.Worksheet != "" {
		rangeStr = fmt.Sprintf("'%s'!%s", cfg.Worksheet, rangeStr)
	}

	endpoint := fmt.Sprintf("%s/%s/values/%s:append?valueInputOption=USER_ENTERED",
		googleSheetsAPIBase,
		url.PathEscape(cfg.SpreadsheetID),
		url.PathEscape(rangeStr),
	)

	// Try parsing RowValues as JSON array
	var row []interface{}
	if err := json.Unmarshal([]byte(cfg.RowValues), &row); err != nil {
		// fallback to comma separated
		parts := strings.Split(cfg.RowValues, ",")
		for _, p := range parts {
			row = append(row, strings.TrimSpace(p))
		}
	}

	bodyData := map[string]interface{}{
		"range":          rangeStr,
		"majorDimension": "ROWS",
		"values": [][]interface{}{
			row,
		},
	}
	body, _ := json.Marshal(bodyData)

	return s.doRequest("POST", endpoint, auth, body)
}
