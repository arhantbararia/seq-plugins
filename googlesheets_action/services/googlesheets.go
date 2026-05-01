package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"googlesheets_action/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

const googleSheetsAPIBase = "https://sheets.googleapis.com/v4/spreadsheets"

type GoogleSheetsService struct {
	httpClient   *http.Client
	retrySeconds int
	retryCount   int
}

func NewGoogleSheetsService() *GoogleSheetsService {
	retrySec := 10
	if val := os.Getenv("RETRY_SECONDS"); val != "" {
		if s, err := strconv.Atoi(val); err == nil {
			retrySec = s
		}
	}
	retryCnt := 3
	if val := os.Getenv("RETRY_COUNT"); val != "" {
		if c, err := strconv.Atoi(val); err == nil {
			retryCnt = c
		}
	}

	return &GoogleSheetsService{
		retrySeconds: retrySec,
		retryCount:   retryCnt,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSHandshakeTimeout: 30 * time.Second,
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dialer := &net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
					}
					return dialer.DialContext(ctx, "tcp4", addr)
				},
				MaxIdleConns:          10,
				IdleConnTimeout:       90 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
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
	clientID := os.Getenv("GSHEETS_CLIENT_ID")
	clientSecret := os.Getenv("GSHEETS_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		return models.AuthData{}, fmt.Errorf("GSHEETS_CLIENT_ID or GSHEETS_CLIENT_SECRET not set")
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

func publishResult(publisher PublisherProvider, task models.ActionTask, resultOutput map[string]interface{}, elapsedMs int64, procErr error, status string, retryCount int) {
	if publisher == nil {
		return
	}
	actionResult := models.ActionResult{
		TaskID:       task.ID,
		WorkflowID:   task.WorkflowID,
		Timestamp:    time.Now().UTC(),
		ResponseTime: elapsedMs,
		Status:       status,
		RetryCount:   retryCount,
	}

	if status == "error" && procErr != nil {
		actionResult.Success = false
		actionResult.Error = procErr.Error()
	} else if status == "success" {
		actionResult.Success = true
		actionResult.Output = resultOutput
	} else if status == "retrying" && procErr != nil {
		actionResult.Success = false
		actionResult.Error = procErr.Error()
	}

	if pubErr := publisher.Publish(task.WorkflowID, actionResult); pubErr != nil {
		log.Printf("Failed to publish action result: %v", pubErr)
	}
}

// HandleTaskRouter dynamically routes to the specific capability method based on CapabilityKey.
// It uses a captured ActionConfig (assigned at setup) to ensure the consumer works
// in its own independent scope.
func (s *GoogleSheetsService) HandleTaskRouter(cfgProvider ConfigProvider, publisher PublisherProvider, seq uint64, instanceID string, initialCfg models.ActionConfig) func(amqp.Delivery) {
	currentCfg := initialCfg

	return func(d amqp.Delivery) {
		var task models.ActionTask
		if err := json.Unmarshal(d.Body, &task); err != nil {
			log.Printf("[Consumer #%d] Error unmarshaling task: %v", seq, err)
			d.Nack(false, false)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Received task: %s", seq, task.WorkflowID, instanceID, task.CapabilityKey)

		// Resolve {{trigger.payload.X}} templates in a COPY of the current config.
		taskCfg := currentCfg
		resolveTemplates(&taskCfg, task.Payload)

		auth, err := s.GetValidAuth(instanceID, cfgProvider)
		if err != nil {
			log.Printf("[Consumer #%d] Error ensuring valid auth: %v", seq, err)
			d.Nack(false, false)
			return
		}

		capability := taskCfg.CapabilityKey

		var procErr error
		var resultOutput map[string]interface{}
		var elapsedMs int64

		// Retry logic
		for attempt := 0; attempt <= s.retryCount; attempt++ {
			switch capability {
			case "googlesheets_update_cell", "googlesheets_update_cell_capability":
				resultOutput, elapsedMs, procErr = s.UpdateCell(auth, taskCfg)
			case "googlesheets_add_row", "googlesheets_add_row_capability":
				resultOutput, elapsedMs, procErr = s.AddRow(auth, taskCfg)
			default:
				log.Printf("[Consumer #%d] Unknown capability key: %s", seq, capability)
				d.Nack(false, false)
				return
			}

			// Handle 401 Refresh logic
			if procErr != nil && strings.Contains(procErr.Error(), "401") {
				log.Printf("[GoogleSheetsService] Action failed with 401, trying immediate refresh and retry for instance %s", instanceID)
				newAuth, refreshErr := s.RefreshAccessToken(auth)
				if refreshErr == nil {
					// Update cache
					for k := range currentCfg.AuthContext {
						currentCfg.AuthContext[k] = newAuth
						break
					}
					_ = cfgProvider.UpdateAuth(instanceID, currentCfg.AuthContext)
					auth = newAuth

					// Retry immediately after refresh
					switch capability {
					case "googlesheets_update_cell", "googlesheets_update_cell_capability":
						resultOutput, elapsedMs, procErr = s.UpdateCell(auth, taskCfg)
					case "googlesheets_add_row", "googlesheets_add_row_capability":
						resultOutput, elapsedMs, procErr = s.AddRow(auth, taskCfg)
					}
				} else {
					log.Printf("[GoogleSheetsService] Refresh failed during retry: %v", refreshErr)
				}
			}

			if procErr == nil {
				// Success
				publishResult(publisher, task, resultOutput, elapsedMs, nil, "success", attempt)
				log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Successfully processed %s on attempt %d", seq, task.WorkflowID, instanceID, capability, attempt)
				d.Ack(false)
				return
			}

			// Failure - decide whether to retry
			if attempt < s.retryCount {
				log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Error on attempt %d for %s: %v. Retrying in %d seconds...", seq, task.WorkflowID, instanceID, attempt, capability, procErr, s.retrySeconds)
				publishResult(publisher, task, nil, elapsedMs, procErr, "retrying", attempt+1)
				time.Sleep(time.Duration(s.retrySeconds) * time.Second)
			} else {
				// All retries failed
				log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] All %d retries failed for %s: %v", seq, task.WorkflowID, instanceID, s.retryCount, capability, procErr)
				publishResult(publisher, task, nil, elapsedMs, procErr, "error", attempt)
				d.Nack(false, false)
				return
			}
		}
	}
}

// ── HTTP request helper ──────────────────────────────────────────────────────

// doRequest executes an HTTP request against the Google Sheets API with
// automatic retry on 429 (rate limit) responses. It retries up to 2 times
// with exponential back-off (1s, 2s).
//
// Google Sheets API: free quota is 300 requests per minute per project for
// read requests and 60 requests per minute per project for write requests.
// No separate payment is required for normal usage.
func (s *GoogleSheetsService) doRequest(method, endpoint string, auth models.AuthData, body []byte) (map[string]interface{}, int64, error) {
	const maxRetries = 2

	var result map[string]interface{}
	var elapsed int64
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		start := time.Now()

		req, err := http.NewRequest(method, endpoint, bytes.NewBuffer(body))
		if err != nil {
			elapsed = time.Since(start).Milliseconds()
			return nil, elapsed, fmt.Errorf("creating http request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+auth.AccessToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := s.httpClient.Do(req)
		elapsed = time.Since(start).Milliseconds()
		if err != nil {
			return nil, elapsed, fmt.Errorf("http request failed: %w", err)
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, elapsed, fmt.Errorf("reading response body: %w", err)
		}

		// Parse response
		result = nil
		if len(respBody) > 0 {
			if jsonErr := json.Unmarshal(respBody, &result); jsonErr != nil {
				// fallback if it's not JSON
				result = map[string]interface{}{"raw": string(respBody)}
			}
		} else {
			result = map[string]interface{}{"status": "success"}
		}

		// Success
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return result, elapsed, nil
		}

		// Rate limited — retry with back-off
		if resp.StatusCode == http.StatusTooManyRequests && attempt < maxRetries {
			backoff := time.Duration(attempt+1) * time.Second
			log.Printf("[doRequest] 429 Rate limited, retrying in %v (attempt %d/%d)", backoff, attempt+1, maxRetries)
			time.Sleep(backoff)
			continue
		}

		// Build error message from API response
		errMsg := "API error"
		if errObj, ok := result["error"].(map[string]interface{}); ok {
			if msg, ok := errObj["message"].(string); ok {
				errMsg = msg
			}
		} else if strings.TrimSpace(string(respBody)) != "" {
			errMsg = string(respBody)
		}
		lastErr = fmt.Errorf("google sheets api returned %d: %s", resp.StatusCode, errMsg)
	}

	return nil, elapsed, lastErr
}

// ── Action: Update Cell ──────────────────────────────────────────────────────

// UpdateCell writes a single value to a specific cell in a Google Sheets
// spreadsheet using the spreadsheets.values.update (PUT) endpoint.
//
// Endpoint:
//
//	PUT https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{range}?valueInputOption=USER_ENTERED
//
// Required config:
//   - spreadsheet_id (string) — the ID extracted from the spreadsheet URL
//   - worksheet      (string) — the sheet/tab name (e.g. "Sheet1")
//   - cell_coordinates (string) — A1 notation of the target cell (e.g. "A1", "B5")
//   - value          (string) — the value to write
//
// Google Sheets API quota: 60 write requests/min/project (free, no payment required).
func (s *GoogleSheetsService) UpdateCell(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	// ── Validate required fields ────────────────────────────────────────
	if cfg.SpreadsheetID == "" {
		return nil, 0, fmt.Errorf("spreadsheet_id is required")
	}
	if cfg.CellCoordinates == "" {
		return nil, 0, fmt.Errorf("cell_coordinates is required (e.g. \"A1\", \"B5\")")
	}

	// ── Build A1 range string ───────────────────────────────────────────
	rangeStr := cfg.CellCoordinates
	if cfg.Worksheet != "" {
		// Single-quote the sheet name to handle names with spaces/special chars.
		rangeStr = fmt.Sprintf("'%s'!%s", cfg.Worksheet, cfg.CellCoordinates)
	}

	// ── Build endpoint URL ──────────────────────────────────────────────
	// The range must be path-escaped because it may contain ! and ' characters.
	endpoint := fmt.Sprintf("%s/%s/values/%s?valueInputOption=USER_ENTERED",
		googleSheetsAPIBase,
		url.PathEscape(cfg.SpreadsheetID),
		url.PathEscape(rangeStr),
	)

	// ── Build request body (ValueRange) ─────────────────────────────────
	bodyData := map[string]interface{}{
		"range":          rangeStr,
		"majorDimension": "ROWS",
		"values": [][]interface{}{
			{cfg.Value},
		},
	}
	body, err := json.Marshal(bodyData)
	if err != nil {
		return nil, 0, fmt.Errorf("marshaling request body: %w", err)
	}

	log.Printf("[UpdateCell] spreadsheet=%s range=%s value=%q",
		cfg.SpreadsheetID, rangeStr, cfg.Value)

	return s.doRequest("PUT", endpoint, auth, body)
}

// ── Action: Add Row ──────────────────────────────────────────────────────────

// AddRow appends a new row of values to a Google Sheets spreadsheet using the
// spreadsheets.values.append (POST) endpoint.
//
// Endpoint:
//
//	POST https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{range}:append
//	     ?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS
//
// Required config:
//   - spreadsheet_id (string) — the ID extracted from the spreadsheet URL
//   - worksheet      (string) — the sheet/tab name (e.g. "Sheet1")
//   - row_values     (string) — either a JSON array (e.g. '["a","b","c"]') or
//     a comma-separated string (e.g. "a, b, c")
//
// The insertDataOption=INSERT_ROWS parameter ensures that existing data is never
// overwritten — new rows are always inserted below the last occupied row.
//
// Google Sheets API quota: 60 write requests/min/project (free, no payment required).
func (s *GoogleSheetsService) AddRow(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	// ── Validate required fields ────────────────────────────────────────
	if cfg.SpreadsheetID == "" {
		return nil, 0, fmt.Errorf("spreadsheet_id is required")
	}
	if cfg.RowValues == "" {
		return nil, 0, fmt.Errorf("row_values is required (JSON array or comma-separated string)")
	}

	// ── Build A1 range for append ───────────────────────────────────────
	// The range tells the API where to look for an existing table. "A1" is the
	// conventional anchor; the API will find the last occupied row and append below.
	rangeStr := "A1"
	if cfg.Worksheet != "" {
		rangeStr = fmt.Sprintf("'%s'!A1", cfg.Worksheet)
	}

	// ── Build endpoint URL ──────────────────────────────────────────────
	// insertDataOption=INSERT_ROWS prevents overwriting existing data.
	endpoint := fmt.Sprintf("%s/%s/values/%s:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS",
		googleSheetsAPIBase,
		url.PathEscape(cfg.SpreadsheetID),
		url.PathEscape(rangeStr),
	)

	// ── Parse row values ────────────────────────────────────────────────
	var row []interface{}
	// First, try parsing as a JSON array (e.g. '["hello", 42, true]').
	if err := json.Unmarshal([]byte(cfg.RowValues), &row); err != nil {
		// Fallback: treat as comma-separated values.
		parts := strings.Split(cfg.RowValues, ",")
		for _, p := range parts {
			row = append(row, strings.TrimSpace(p))
		}
	}

	if len(row) == 0 {
		return nil, 0, fmt.Errorf("row_values resolved to an empty row — nothing to append")
	}

	// ── Build request body (ValueRange) ─────────────────────────────────
	bodyData := map[string]interface{}{
		"range":          rangeStr,
		"majorDimension": "ROWS",
		"values": [][]interface{}{
			row,
		},
	}
	body, err := json.Marshal(bodyData)
	if err != nil {
		return nil, 0, fmt.Errorf("marshaling request body: %w", err)
	}

	log.Printf("[AddRow] spreadsheet=%s worksheet=%s rowCells=%d",
		cfg.SpreadsheetID, cfg.Worksheet, len(row))

	return s.doRequest("POST", endpoint, auth, body)
}
