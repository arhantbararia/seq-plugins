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

	"slack_action/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ── Slack Web API base ───────────────────────────────────────────────────────

const slackAPIBase = "https://slack.com/api"

// ── Service ──────────────────────────────────────────────────────────────────

// SlackService wraps the Slack Web API and handles RabbitMQ action tasks.
type SlackService struct {
	httpClient   *http.Client
	retrySeconds int
	retryCount   int
}

// NewSlackService constructs a SlackService with a robust HTTP client (IPv4-only for HF Spaces).
func NewSlackService() *SlackService {
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

	return &SlackService{
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

// ── Interfaces ───────────────────────────────────────────────────────────────

// ConfigProvider defines an interface to retrieve an ActionConfig by instance ID.
type ConfigProvider interface {
	GetConfig(id string) (models.ActionConfig, error)
	UpdateAuth(id string, auth map[string]models.AuthData) error
}

// PublisherProvider defines an interface to publish ActionResults to RabbitMQ.
type PublisherProvider interface {
	Publish(workflowID string, result models.ActionResult) error
}

// ── Template resolution ──────────────────────────────────────────────────────

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
	if v, ok := cfg.RawConfig["channel"].(string); ok {
		cfg.Channel = v
	}
	if v, ok := cfg.RawConfig["message"].(string); ok {
		cfg.Message = v
	}
	if v, ok := cfg.RawConfig["attachments"].(string); ok {
		cfg.Attachments = v
	}
	if v, ok := cfg.RawConfig["link"].(string); ok {
		cfg.Link = v
	}
}

// ── Auth helpers ─────────────────────────────────────────────────────────────

func getAuth(ctx map[string]models.AuthData) models.AuthData {
	for _, a := range ctx {
		return a
	}
	return models.AuthData{}
}

// GetValidAuth returns the OAuth2 access token from the auth context.
// Slack Bot Tokens (xoxb-) do not expire, so no refresh logic is needed.
func (s *SlackService) GetValidAuth(id string, cfgProvider ConfigProvider, cfg models.ActionConfig) (models.AuthData, error) {
	return getAuth(cfg.AuthContext), nil
}

// ── Result publishing ────────────────────────────────────────────────────────

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

// ── RabbitMQ Task Router ─────────────────────────────────────────────────────

// HandleTaskRouter dynamically routes to the specific capability method based on CapabilityKey.
// It uses a captured ActionConfig (assigned at setup) to ensure the consumer works
// in its own independent scope, ignoring any randomized IDs sent by triggers.
func (s *SlackService) HandleTaskRouter(cfgProvider ConfigProvider, publisher PublisherProvider, seq uint64, instanceID string, initialCfg models.ActionConfig) func(amqp.Delivery) {
	// Each consumer instance keeps its own copy of the configuration assigned at setup.
	currentCfg := initialCfg

	return func(d amqp.Delivery) {
		var task models.ActionTask
		if err := json.Unmarshal(d.Body, &task); err != nil {
			log.Printf("[Consumer #%d] Error unmarshaling task: %v", seq, err)
			d.Nack(false, false)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Received task: %s (Target Instance: %s)", seq, task.WorkflowID, task.ID, task.CapabilityKey, instanceID)

		// Resolve {{trigger.payload.X}} templates in a copy of the assigned config to avoid mutation.
		taskCfg := currentCfg
		resolveTemplates(&taskCfg, task.Payload)

		auth, err := s.GetValidAuth(instanceID, cfgProvider, taskCfg)
		if err != nil {
			log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Error ensuring valid auth: %v", seq, task.WorkflowID, instanceID, err)
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
			case "slack_post_message", "slack_post_message_capability":
				resultOutput, elapsedMs, procErr = s.PostMessage(auth, taskCfg)
			default:
				log.Printf("[Consumer #%d] [Workflow: %s] [Action: %s] Unknown capability key: %s", seq, task.WorkflowID, task.ID, capability)
				d.Nack(false, false)
				return
			}

			if procErr == nil {
				// Success
				publishResult(publisher, task, resultOutput, elapsedMs, nil, "success", attempt)
				log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Successfully processed %s on attempt %d", seq, task.WorkflowID, instanceID, capability, attempt)
				d.Ack(false)
				return
			}

			// Failure — decide whether to retry
			if attempt < s.retryCount {
				log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Error on attempt %d for %s: %v. Retrying in %d seconds...", seq, task.WorkflowID, instanceID, attempt, capability, procErr, s.retrySeconds)
				publishResult(publisher, task, nil, elapsedMs, procErr, "retrying", attempt+1)
				time.Sleep(time.Duration(s.retrySeconds) * time.Second)
			} else {
				// All retries failed
				log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] All %d retries failed for %s: %v", seq, task.WorkflowID, instanceID, s.retryCount, capability, procErr)
				publishResult(publisher, task, nil, elapsedMs, procErr, "error", attempt)
				d.Nack(false, false) // Don't requeue, we've exhausted retries
			}
		}
	}
}

// ── Core Capability: Post Message to Channel ─────────────────────────────────

// PostMessage posts a message to a Slack channel using the chat.postMessage API.
//
// Slack API: POST https://slack.com/api/chat.postMessage
// Auth: Bearer token (Bot User OAuth Token — xoxb-...)
// Required scope: chat:write  (+ chat:write.public to post in channels the bot hasn't joined)
//
// Request body (JSON):
//   - channel (string, required): Channel ID or name (e.g. "C0123456789" or "#general")
//   - text (string, required as fallback): The message text. Acts as notification fallback when blocks are present.
//   - attachments (array, optional): Legacy rich-message attachments.
//
// If a "link" is provided in config, it is appended to the message text.
func (s *SlackService) PostMessage(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.Channel == "" {
		return nil, 0, fmt.Errorf("channel is required")
	}
	messageText := cfg.Message
	if messageText == "" {
		return nil, 0, fmt.Errorf("message text is required")
	}

	// Append link to message body if provided
	if cfg.Link != "" {
		messageText = messageText + "\n" + cfg.Link
	}

	// Build request body as JSON
	body := map[string]interface{}{
		"channel": cfg.Channel,
		"text":    messageText,
	}

	// If attachments are provided as a JSON string, parse and include them
	if cfg.Attachments != "" {
		var attachments []interface{}
		if err := json.Unmarshal([]byte(cfg.Attachments), &attachments); err != nil {
			log.Printf("[SlackService] Warning: could not parse attachments JSON, skipping: %v", err)
		} else {
			body["attachments"] = attachments
		}
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshaling request body: %w", err)
	}

	// Determine the token to use
	token := auth.AccessToken
	if token == "" {
		token = auth.BotToken
	}
	if token == "" {
		token = auth.APIKey
	}
	if token == "" {
		return nil, 0, fmt.Errorf("no valid Slack token found in auth context")
	}

	// Execute request
	endpoint := slackAPIBase + "/chat.postMessage"
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, 0, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", "Bearer "+token)

	start := time.Now()
	resp, err := s.httpClient.Do(req)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		return nil, elapsed, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, elapsed, fmt.Errorf("reading response body: %w", err)
	}

	// Slack always returns 200 OK with {"ok": true/false} in the body
	var slackResp map[string]interface{}
	if err := json.Unmarshal(respBody, &slackResp); err != nil {
		return nil, elapsed, fmt.Errorf("unmarshaling response: %w (body: %s)", err, string(respBody))
	}

	// Check Slack's application-level success
	if ok, exists := slackResp["ok"]; exists {
		if okBool, isBool := ok.(bool); isBool && !okBool {
			slackErr, _ := slackResp["error"].(string)
			return nil, elapsed, fmt.Errorf("slack API error: %s", slackErr)
		}
	}

	// Build output
	output := map[string]interface{}{
		"ok":      slackResp["ok"],
		"channel": slackResp["channel"],
		"ts":      slackResp["ts"],
	}
	if msg, ok := slackResp["message"]; ok {
		output["message"] = msg
	}

	return output, elapsed, nil
}
