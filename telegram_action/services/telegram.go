package services

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"telegram_action/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

const telegramAPIBase = "https://api.telegram.org/bot"

// TelegramService wraps the Telegram Bot API and handles RabbitMQ tasks.
type TelegramService struct {
	httpClient *http.Client
}

// NewTelegramService constructs a TelegramService with a default HTTP client.
func NewTelegramService() *TelegramService {
	return &TelegramService{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

type telegramResponse struct {
	OK          bool            `json:"ok"`
	Description string          `json:"description,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
}

func apiURL(botToken, method string) string {
	return fmt.Sprintf("%s%s/%s", telegramAPIBase, botToken, method)
}

func (s *TelegramService) doPost(endpoint string, params url.Values) (json.RawMessage, int64, error) {
	start := time.Now()
	resp, err := s.httpClient.Post(endpoint, "application/x-www-form-urlencoded", strings.NewReader(params.Encode()))
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		return nil, elapsed, fmt.Errorf("http post failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, elapsed, fmt.Errorf("reading response body: %w", err)
	}

	var tResp telegramResponse
	if err := json.Unmarshal(body, &tResp); err != nil {
		return nil, elapsed, fmt.Errorf("unmarshalling telegram response: %w", err)
	}
	if !tResp.OK {
		return nil, elapsed, fmt.Errorf("telegram API error: %s", tResp.Description)
	}
	return tResp.Result, elapsed, nil
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
	if v, ok := cfg.RawConfig["chat_id"].(string); ok {
		cfg.ChatID = v
	}
	if v, ok := cfg.RawConfig["message"].(string); ok {
		cfg.Message = v
	}
	if v, ok := cfg.RawConfig["text"].(string); ok {
		cfg.Text = v
	}
	if v, ok := cfg.RawConfig["caption"].(string); ok {
		cfg.Caption = v
	}
	if v, ok := cfg.RawConfig["file_url"].(string); ok {
		cfg.FileURL = v
	}
}

// ConfigProvider defines an interface to retrieve an ActionConfig by ID
type ConfigProvider interface {
	GetConfig(id string) (models.ActionConfig, error)
	UpdateAuth(id string, auth map[string]models.AuthData) error
}

// GetValidAuth placeholder for Telegram (no refresh needed)
func (s *TelegramService) GetValidAuth(id string, cfgProvider ConfigProvider) (models.AuthData, error) {
	cfg, err := cfgProvider.GetConfig(id)
	if err != nil {
		return models.AuthData{}, err
	}
	return getAuth(cfg.AuthContext), nil
}

// PublisherProvider defines an interface to publish ActionResults
type PublisherProvider interface {
	Publish(workflowID string, result models.ActionResult) error
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

// -------------------------------------------------------------------
// RabbitMQ Handlers
// -------------------------------------------------------------------

// HandleTaskRouter dynamically routes to the specific capability method based on CapabilityKey
func (s *TelegramService) HandleTaskRouter(cfgProvider ConfigProvider, publisher PublisherProvider, seq uint64) func(amqp.Delivery) {
	return func(d amqp.Delivery) {
		var task models.ActionTask
		if err := json.Unmarshal(d.Body, &task); err != nil {
			log.Printf("[Consumer #%d] Error unmarshaling task: %v", seq, err)
			d.Nack(false, false)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Received task: %s", seq, task.WorkflowID, task.ID, task.CapabilityKey)

		// Use task.ID (Action Instance ID) to get the correct config
		cfg, err := cfgProvider.GetConfig(task.ID)
		if err != nil {
			log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Error fetching config: %v", seq, task.WorkflowID, task.ID, err)
			d.Nack(false, false)
			return
		}

		resolveTemplates(&cfg, task.Payload)
		auth, err := s.GetValidAuth(task.ID, cfgProvider)
		if err != nil {
			log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Error ensuring valid auth: %v", seq, task.WorkflowID, task.ID, err)
			d.Nack(false, false)
			return
		}

		capability := cfg.CapabilityKey

		var procErr error
		var resultOutput map[string]interface{}
		var elapsedMs int64

		switch capability {
		case "telegram_send_message", "telegram_send_message_capability":
			resultOutput, elapsedMs, procErr = s.SendMessage(auth, cfg)
		case "telegram_send_photo", "telegram_send_photo_capability":
			resultOutput, elapsedMs, procErr = s.SendPhoto(auth, cfg)
		case "telegram_send_video", "telegram_send_video_capability":
			resultOutput, elapsedMs, procErr = s.SendVideo(auth, cfg)
		case "telegram_send_mp3", "telegram_send_mp3_capability":
			resultOutput, elapsedMs, procErr = s.SendMP3(auth, cfg)
		default:
			log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Unknown capability key: %s", seq, task.WorkflowID, task.ID, capability)
			d.Nack(false, false)
			return
		}

		publishResult(publisher, task, resultOutput, elapsedMs, procErr)

		if procErr != nil {
			log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Error processing capability %s: %v", seq, task.WorkflowID, task.ID, capability, procErr)
			d.Nack(false, true)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Successfully processed %s", seq, task.WorkflowID, task.ID, capability)
		d.Ack(false)
	}
}

func getAuth(ctx map[string]models.AuthData) models.AuthData {
	for _, a := range ctx {
		// Fallback for Goat-Backend API Key logic which stores tokens under "api_key"
		if a.BotToken == "" && a.APIKey != "" {
			a.BotToken = a.APIKey
		}
		return a
	}
	return models.AuthData{}
}

// -------------------------------------------------------------------
// Core Capabilities Implementations
// -------------------------------------------------------------------

func (s *TelegramService) SendMessage(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	text := cfg.Message
	if text == "" {
		text = cfg.Text
	}
	if cfg.ChatID == "" {
		return nil, 0, fmt.Errorf("chat_id is required")
	}
	if text == "" {
		return nil, 0, fmt.Errorf("message text is required")
	}

	params := url.Values{
		"chat_id":    {cfg.ChatID},
		"text":       {text},
		"parse_mode": {"HTML"},
	}

	result, elapsed, err := s.doPost(apiURL(auth.BotToken, "sendMessage"), params)
	if err != nil {
		return nil, elapsed, err
	}

	var msg map[string]interface{}
	_ = json.Unmarshal(result, &msg)
	return msg, elapsed, nil
}

func (s *TelegramService) SendPhoto(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.ChatID == "" {
		return nil, 0, fmt.Errorf("chat_id is required")
	}
	if cfg.FileURL == "" {
		return nil, 0, fmt.Errorf("file_url is required")
	}

	params := url.Values{
		"chat_id": {cfg.ChatID},
		"photo":   {cfg.FileURL},
	}
	if cfg.Caption != "" {
		params.Set("caption", cfg.Caption)
		params.Set("parse_mode", "HTML")
	}

	result, elapsed, err := s.doPost(apiURL(auth.BotToken, "sendPhoto"), params)
	if err != nil {
		return nil, elapsed, err
	}

	var msg map[string]interface{}
	_ = json.Unmarshal(result, &msg)
	return msg, elapsed, nil
}

func (s *TelegramService) SendVideo(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.ChatID == "" {
		return nil, 0, fmt.Errorf("chat_id is required")
	}
	if cfg.FileURL == "" {
		return nil, 0, fmt.Errorf("file_url is required")
	}

	params := url.Values{
		"chat_id": {cfg.ChatID},
		"video":   {cfg.FileURL},
	}
	if cfg.Caption != "" {
		params.Set("caption", cfg.Caption)
		params.Set("parse_mode", "HTML")
	}

	result, elapsed, err := s.doPost(apiURL(auth.BotToken, "sendVideo"), params)
	if err != nil {
		return nil, elapsed, err
	}

	var msg map[string]interface{}
	_ = json.Unmarshal(result, &msg)
	return msg, elapsed, nil
}

func (s *TelegramService) SendMP3(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.ChatID == "" {
		return nil, 0, fmt.Errorf("chat_id is required")
	}
	if cfg.FileURL == "" {
		return nil, 0, fmt.Errorf("file_url is required")
	}

	params := url.Values{
		"chat_id": {cfg.ChatID},
		"audio":   {cfg.FileURL},
	}
	if cfg.Caption != "" {
		params.Set("caption", cfg.Caption)
		params.Set("parse_mode", "HTML")
	}

	result, elapsed, err := s.doPost(apiURL(auth.BotToken, "sendAudio"), params)
	if err != nil {
		return nil, elapsed, err
	}

	var msg map[string]interface{}
	_ = json.Unmarshal(result, &msg)
	return msg, elapsed, nil
}
