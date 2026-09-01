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
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"openwa_action/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

// OpenWAService wraps the OpenWA REST API and handles RabbitMQ tasks.
type OpenWAService struct {
	httpClient     *http.Client
	retrySeconds   int
	retryCount     int
	baseURL        string
	defaultSession string
}

// NewOpenWAService constructs an OpenWAService with a robust HTTP client.
func NewOpenWAService() *OpenWAService {
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

	baseURL := os.Getenv("OPENWA_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:2785"
	}

	defaultSession := os.Getenv("OPENWA_DEFAULT_SESSION_ID")

	return &OpenWAService{
		retrySeconds:   retrySec,
		retryCount:     retryCnt,
		baseURL:        strings.TrimRight(baseURL, "/"),
		defaultSession: defaultSession,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				TLSHandshakeTimeout: 10 * time.Second,
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dialer := &net.Dialer{
						Timeout:   10 * time.Second,
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

// doRequest sends a JSON request to the OpenWA API with retry and rate-limit handling.
func (s *OpenWAService) doRequest(method, path, apiKey string, payload interface{}) (map[string]interface{}, int64, error) {
	start := time.Now()
	endpoint := s.baseURL + "/api" + path

	var bodyBytes []byte
	if payload != nil {
		var err error
		bodyBytes, err = json.Marshal(payload)
		if err != nil {
			return nil, time.Since(start).Milliseconds(), fmt.Errorf("marshalling request body: %w", err)
		}
	}

	var resp *http.Response
	var err error
	maxRetries := 3
	backoff := 2 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		var req *http.Request
		if payload != nil {
			req, err = http.NewRequest(method, endpoint, bytes.NewReader(bodyBytes))
		} else {
			req, err = http.NewRequest(method, endpoint, nil)
		}

		if err != nil {
			return nil, time.Since(start).Milliseconds(), fmt.Errorf("creating request: %w", err)
		}

		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if apiKey != "" {
			req.Header.Set("X-API-Key", apiKey)
		}

		resp, err = s.httpClient.Do(req)
		if err == nil {
			// Rate limit handling (HTTP 429)
			if resp.StatusCode == http.StatusTooManyRequests {
				retryAfter := resp.Header.Get("Retry-After")
				resp.Body.Close()

				waitDuration := backoff
				if retryAfter != "" {
					if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
						waitDuration = time.Duration(seconds) * time.Second
					}
				}
				if attempt < maxRetries {
					log.Printf("[OpenWA] Rate limited (429). Waiting %v before retry %d...", waitDuration, attempt+1)
					time.Sleep(waitDuration)
					continue
				}
				return nil, time.Since(start).Milliseconds(), fmt.Errorf("rate limited (429) after %d retries", maxRetries)
			}
			break
		}

		if attempt < maxRetries {
			log.Printf("[OpenWA] HTTP %s failed on attempt %d: %v. Retrying in %v...", method, attempt, err, backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		return nil, elapsed, fmt.Errorf("http request failed after %d retries: %w", maxRetries, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, elapsed, fmt.Errorf("reading response body: %w", err)
	}

	// Parse response
	var result map[string]interface{}
	if len(respBody) > 0 {
		// OpenWA returns raw arrays for list endpoints, but action endpoints return objects
		if err := json.Unmarshal(respBody, &result); err != nil {
			// If it's not a JSON object, maybe it's just a raw response or an array
			return nil, elapsed, fmt.Errorf("unmarshalling response: %w (body: %s)", err, string(respBody))
		}
	} else {
		result = map[string]interface{}{}
	}

	// API errors (4xx, 5xx)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errMsg := string(respBody)
		if msg, ok := result["message"].(string); ok {
			errMsg = msg
		} else if msgs, ok := result["message"].([]interface{}); ok && len(msgs) > 0 {
			if strMsg, ok := msgs[0].(string); ok {
				errMsg = strMsg
			}
		}
		return nil, elapsed, fmt.Errorf("OpenWA API error (HTTP %d): %s", resp.StatusCode, errMsg)
	}

	return result, elapsed, nil
}

// formatChatID takes a raw phone number or group ID and formats it for OpenWA.
func formatChatID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return id
	}
	// If it contains a template variable, do not format it yet
	if strings.Contains(id, "{{") && strings.Contains(id, "}}") {
		return id
	}
	if strings.Contains(id, "@") {
		return id // Already has @c.us or @g.us
	}
	
	// If no '@', assume it's a phone number. Strip non-digits.
	var builder strings.Builder
	for _, r := range id {
		if r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	digitsOnly := builder.String()
	if digitsOnly == "" {
		return id + "@c.us"
	}
	return digitsOnly + "@c.us"
}

// FormatChatIDList formats a comma-separated string of chat IDs and joins them back.
func FormatChatIDList(phoneStr string) string {
	parts := strings.Split(phoneStr, ",")
	var formatted []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			formatted = append(formatted, formatChatID(trimmed))
		}
	}
	return strings.Join(formatted, ",")
}

// parsePhoneNumbers splits a comma-separated chat ID string into a trimmed list.
func parsePhoneNumbers(phoneStr string) []string {
	parts := strings.Split(phoneStr, ",")
	var numbers []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			numbers = append(numbers, formatChatID(trimmed))
		}
	}
	return numbers
}

type sendResult struct {
	ChatID   string                 `json:"chat_id"`
	Success  bool                   `json:"success"`
	Response map[string]interface{} `json:"response,omitempty"`
	Error    string                 `json:"error,omitempty"`
}

func aggregateResults(results []sendResult) map[string]interface{} {
	successful := 0
	failed := 0
	for _, r := range results {
		if r.Success {
			successful++
		} else {
			failed++
		}
	}

	resultList := make([]interface{}, len(results))
	for i, r := range results {
		item := map[string]interface{}{
			"chat_id": r.ChatID,
			"success": r.Success,
		}
		if r.Response != nil {
			item["response"] = r.Response
		}
		if r.Error != "" {
			item["error"] = r.Error
		}
		resultList[i] = item
	}

	return map[string]interface{}{
		"results":    resultList,
		"total":      len(results),
		"successful": successful,
		"failed":     failed,
	}
}

// ─── Template Resolution ─────────────────────────────────────────────────────

var templatePattern = regexp.MustCompile(`\{\{trigger\.payload\.(\w+)\}\}`)

func resolveTemplates(cfg *models.ActionConfig, payload map[string]interface{}) {
	if payload == nil || cfg.RawConfig == nil {
		return
	}

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
			return match
		})
	}

	for k, v := range cfg.RawConfig {
		if s, ok := v.(string); ok {
			cfg.RawConfig[k] = resolveString(s)
		}
	}

	// Re-extract fields
	if v, ok := cfg.RawConfig["chat_id"].(string); ok {
		cfg.ChatID = FormatChatIDList(v)
	}
	if v, ok := cfg.RawConfig["text"].(string); ok {
		cfg.Text = v
	}
	if v, ok := cfg.RawConfig["url"].(string); ok {
		cfg.URL = v
	}
	if v, ok := cfg.RawConfig["caption"].(string); ok {
		cfg.Caption = v
	}
	if v, ok := cfg.RawConfig["filename"].(string); ok {
		cfg.Filename = v
	}
	if v, ok := cfg.RawConfig["mimetype"].(string); ok {
		cfg.Mimetype = v
	}
	if v, ok := cfg.RawConfig["latitude"].(string); ok {
		cfg.Latitude = v
	}
	if v, ok := cfg.RawConfig["longitude"].(string); ok {
		cfg.Longitude = v
	}
	if v, ok := cfg.RawConfig["description"].(string); ok {
		cfg.Description = v
	}
	if v, ok := cfg.RawConfig["contact_name"].(string); ok {
		cfg.ContactName = v
	}
	if v, ok := cfg.RawConfig["contact_number"].(string); ok {
		cfg.ContactNumber = v
	}
	if v, ok := cfg.RawConfig["template_name"].(string); ok {
		cfg.TemplateName = v
	}
	if v, ok := cfg.RawConfig["template_vars"].(string); ok {
		cfg.TemplateVars = v
	}
	if v, ok := cfg.RawConfig["state"].(string); ok {
		cfg.State = v
	}
	if v, ok := cfg.RawConfig["ptt"].(bool); ok {
		cfg.PTT = v
	} else if v, ok := cfg.RawConfig["ptt"].(string); ok {
		cfg.PTT = v == "true"
	}
}

// ─── Auth Helpers ────────────────────────────────────────────────────────────

type ConfigProvider interface {
	GetConfig(id string) (models.ActionConfig, error)
	UpdateAuth(id string, auth map[string]models.AuthData) error
}

func getAuth(ctx map[string]models.AuthData) models.AuthData {
	for _, a := range ctx {
		return a
	}
	return models.AuthData{}
}

func (s *OpenWAService) resolveSessionID(auth models.AuthData, cfg models.ActionConfig) (string, error) {
	if auth.ExternalAccountID != "" {
		return auth.ExternalAccountID, nil
	}
	if s.defaultSession != "" {
		return s.defaultSession, nil
	}
	return "", fmt.Errorf("no session ID available: set session_id in config, link a WhatsApp account, or set OPENWA_DEFAULT_SESSION_ID")
}

func (s *OpenWAService) resolveAPIKey(auth models.AuthData) string {
	if auth.APIKey != "" {
		return auth.APIKey
	}
	if auth.AccessToken != "" {
		return auth.AccessToken
	}
	// Fallback to bot API key if provided
	return os.Getenv("OPENWA_BOT_API_KEY")
}

type PublisherProvider interface {
	Publish(workflowID string, result models.ActionResult) error
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

// ─── RabbitMQ Task Router ────────────────────────────────────────────────────

func (s *OpenWAService) HandleTaskRouter(cfgProvider ConfigProvider, publisher PublisherProvider, seq uint64, instanceID string, initialCfg models.ActionConfig) func(amqp.Delivery) {
	currentCfg := initialCfg

	return func(d amqp.Delivery) {
		var task models.ActionTask
		if err := json.Unmarshal(d.Body, &task); err != nil {
			log.Printf("[Consumer #%d] Error unmarshaling task: %v", seq, err)
			d.Nack(false, false)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Received task: %s", seq, task.WorkflowID, task.ID, task.CapabilityKey)

		taskCfg := currentCfg
		resolveTemplates(&taskCfg, task.Payload)
		auth := getAuth(taskCfg.AuthContext)

		capability := taskCfg.CapabilityKey
		var procErr error
		var resultOutput map[string]interface{}
		var elapsedMs int64

		for attempt := 0; attempt <= s.retryCount; attempt++ {
			switch capability {
			case "openwa_send_text":
				resultOutput, elapsedMs, procErr = s.SendText(auth, taskCfg)
			case "openwa_send_image":
				resultOutput, elapsedMs, procErr = s.SendImage(auth, taskCfg)
			case "openwa_send_video":
				resultOutput, elapsedMs, procErr = s.SendVideo(auth, taskCfg)
			case "openwa_send_audio":
				resultOutput, elapsedMs, procErr = s.SendAudio(auth, taskCfg)
			case "openwa_send_document":
				resultOutput, elapsedMs, procErr = s.SendDocument(auth, taskCfg)
			case "openwa_send_location":
				resultOutput, elapsedMs, procErr = s.SendLocation(auth, taskCfg)
			case "openwa_send_contact":
				resultOutput, elapsedMs, procErr = s.SendContact(auth, taskCfg)
			case "openwa_send_template":
				resultOutput, elapsedMs, procErr = s.SendTemplate(auth, taskCfg)
			case "openwa_send_sticker":
				resultOutput, elapsedMs, procErr = s.SendSticker(auth, taskCfg)
			case "openwa_send_typing":
				resultOutput, elapsedMs, procErr = s.SendTyping(auth, taskCfg)
			default:
				log.Printf("[Consumer #%d] Unknown capability key: %s", seq, capability)
				d.Nack(false, false)
				return
			}

			if procErr == nil {
				publishResult(publisher, task, resultOutput, elapsedMs, nil, "success", attempt)
				d.Ack(false)
				return
			}

			if attempt < s.retryCount {
				log.Printf("[Consumer #%d] Retry %d for %s: %v", seq, attempt+1, capability, procErr)
				publishResult(publisher, task, nil, elapsedMs, procErr, "retrying", attempt+1)
				time.Sleep(time.Duration(s.retrySeconds) * time.Second)
			} else {
				log.Printf("[Consumer #%d] Failed after %d retries: %v", seq, s.retryCount, procErr)
				publishResult(publisher, task, nil, elapsedMs, procErr, "error", attempt)
				d.Nack(false, false)
			}
		}
	}
}

// ─── Core Capability Implementations ─────────────────────────────────────────

func (s *OpenWAService) processMultiSend(auth models.AuthData, cfg models.ActionConfig, buildPayload func(chat string) map[string]interface{}, endpoint string) (map[string]interface{}, int64, error) {
	if cfg.ChatID == "" {
		return nil, 0, fmt.Errorf("chat_id is required")
	}

	sessionID, err := s.resolveSessionID(auth, cfg)
	if err != nil {
		return nil, 0, err
	}
	apiKey := s.resolveAPIKey(auth)

	chats := parsePhoneNumbers(cfg.ChatID)
	fullPath := fmt.Sprintf("/sessions/%s/%s", sessionID, endpoint)
	
	start := time.Now()
	var results []sendResult

	for _, chat := range chats {
		payload := buildPayload(chat)
		res, _, err := s.doRequest("POST", fullPath, apiKey, payload)
		if err != nil {
			results = append(results, sendResult{ChatID: chat, Success: false, Error: err.Error()})
		} else {
			results = append(results, sendResult{ChatID: chat, Success: true, Response: res})
		}
	}

	elapsed := time.Since(start).Milliseconds()
	output := aggregateResults(results)

	if output["successful"].(int) == 0 {
		return output, elapsed, fmt.Errorf("all requests failed")
	}
	return output, elapsed, nil
}

func (s *OpenWAService) SendText(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.Text == "" {
		return nil, 0, fmt.Errorf("text is required")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		return map[string]interface{}{"chatId": chat, "text": cfg.Text}
	}, "messages/send-text")
}

func (s *OpenWAService) SendImage(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.URL == "" {
		return nil, 0, fmt.Errorf("url is required")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		p := map[string]interface{}{"chatId": chat, "url": cfg.URL}
		if cfg.Caption != "" {
			p["caption"] = cfg.Caption
		}
		return p
	}, "messages/send-image")
}

func (s *OpenWAService) SendVideo(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.URL == "" {
		return nil, 0, fmt.Errorf("url is required")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		p := map[string]interface{}{"chatId": chat, "url": cfg.URL}
		if cfg.Caption != "" {
			p["caption"] = cfg.Caption
		}
		return p
	}, "messages/send-video")
}

func (s *OpenWAService) SendAudio(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.URL == "" {
		return nil, 0, fmt.Errorf("url is required")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		p := map[string]interface{}{"chatId": chat, "url": cfg.URL}
		if cfg.PTT {
			p["ptt"] = true
		}
		return p
	}, "messages/send-audio")
}

func (s *OpenWAService) SendDocument(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.URL == "" {
		return nil, 0, fmt.Errorf("url is required")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		p := map[string]interface{}{"chatId": chat, "url": cfg.URL}
		if cfg.Caption != "" {
			p["caption"] = cfg.Caption
		}
		if cfg.Filename != "" {
			p["filename"] = cfg.Filename
		}
		return p
	}, "messages/send-document")
}

func (s *OpenWAService) SendLocation(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.Latitude == "" || cfg.Longitude == "" {
		return nil, 0, fmt.Errorf("latitude and longitude are required")
	}
	lat, _ := strconv.ParseFloat(cfg.Latitude, 64)
	lon, _ := strconv.ParseFloat(cfg.Longitude, 64)
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		p := map[string]interface{}{"chatId": chat, "latitude": lat, "longitude": lon}
		if cfg.Description != "" {
			p["description"] = cfg.Description
		}
		return p
	}, "messages/send-location")
}

func (s *OpenWAService) SendContact(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.ContactName == "" || cfg.ContactNumber == "" {
		return nil, 0, fmt.Errorf("contact_name and contact_number are required")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		return map[string]interface{}{
			"chatId":        chat,
			"contactName":   cfg.ContactName,
			"contactNumber": cfg.ContactNumber,
		}
	}, "messages/send-contact")
}

func (s *OpenWAService) SendTemplate(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.TemplateName == "" {
		return nil, 0, fmt.Errorf("template_name is required")
	}
	var vars map[string]string
	if cfg.TemplateVars != "" {
		if err := json.Unmarshal([]byte(cfg.TemplateVars), &vars); err != nil {
			return nil, 0, fmt.Errorf("invalid template_vars JSON: %v", err)
		}
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		p := map[string]interface{}{"chatId": chat, "templateName": cfg.TemplateName}
		if vars != nil {
			p["vars"] = vars
		}
		return p
	}, "messages/send-template")
}

func (s *OpenWAService) SendSticker(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.URL == "" {
		return nil, 0, fmt.Errorf("url is required")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		p := map[string]interface{}{"chatId": chat, "url": cfg.URL}
		if cfg.Mimetype != "" {
			p["mimetype"] = cfg.Mimetype
		}
		return p
	}, "messages/send-sticker")
}

func (s *OpenWAService) SendTyping(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.State == "" {
		return nil, 0, fmt.Errorf("state is required (typing, recording, paused)")
	}
	return s.processMultiSend(auth, cfg, func(chat string) map[string]interface{} {
		return map[string]interface{}{"chatId": chat, "state": cfg.State}
	}, "chats/typing")
}
