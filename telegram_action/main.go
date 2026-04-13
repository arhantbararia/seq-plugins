package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"telegram_action/models"
	"telegram_action/services"
	"telegram_action/worker"
	"time"
)

// Global state
var (
	// consumers map stores active consumers, keyed by workflow ID.
	consumers = make(map[string]*worker.Consumer)
	// configs stores action configurations, keyed by action ID.
	configs = &sync.Map{}
	// mu protects the consumers map.
	mu sync.Mutex

	// telegramSvc is the service for interacting with the Telegram API.
	// This is initialized in main().
	telegramSvc *services.TelegramService
	// resultPublisher handles publishing ActionResults to RabbitMQ
	resultPublisher *worker.Publisher

	consumerCount uint64
)

// workflowConfigProvider implements the services.ConfigProvider interface,
// allowing RabbitMQ handlers to retrieve configuration for a workflow.
type workflowConfigProvider struct{}

// GetConfig retrieves the ActionConfig for a given action ID from the global store.
func (p *workflowConfigProvider) GetConfig(id string) (models.ActionConfig, error) {
	val, ok := configs.Load(id)
	if !ok {
		return models.ActionConfig{}, fmt.Errorf("config not found for action %s", id)
	}
	cfg, ok := val.(models.ActionConfig)
	if !ok {
		return models.ActionConfig{}, fmt.Errorf("invalid config type in store for action %s", id)
	}
	return cfg, nil
}

func (p *workflowConfigProvider) UpdateAuth(id string, auth map[string]models.AuthData) error {
	val, ok := configs.Load(id)
	if !ok {
		return fmt.Errorf("config not found for action %s", id)
	}
	cfg, ok := val.(models.ActionConfig)
	if !ok {
		return fmt.Errorf("invalid config type in store for action %s", id)
	}
	cfg.AuthContext = auth
	configs.Store(id, cfg)
	return nil
}

type SetupPayload struct {
	ID            string                 `json:"id"`
	WorkflowID    string                 `json:"workflow_id"`
	QueueName     string                 `json:"queue_name"`
	CapabilityKey string                 `json:"capability_key"`
	Config        map[string]interface{} `json:"config"`
}

type RemovePayload struct {
	ID         string `json:"id"`
	WorkflowID string `json:"workflow_id"`
}

func stringField(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}




// buildActionConfig extracts typed fields from the raw config map.
func buildActionConfig(req SetupPayload) models.ActionConfig {
	cfg := models.ActionConfig{
		CapabilityKey: req.CapabilityKey,
	}

	if authCtxRaw, ok := req.Config["_auth_context"]; ok {
		if authCtx, ok := authCtxRaw.(map[string]models.AuthData); ok {
			cfg.AuthContext = authCtx
		} else {
			cfg.AuthContext = extractAuthContext(req.Config)
		}
	} else {
		cfg.AuthContext = extractAuthContext(req.Config)
	}

	// Store the full raw config for template resolution at runtime.
	rawCfg := make(map[string]interface{})
	for k, v := range req.Config {
		if k != "_auth_context" {
			rawCfg[k] = v
		}
	}
	cfg.RawConfig = rawCfg

	// Extract string fields from config map
	cfg.ChatID = stringField(req.Config, "chat_id")
	cfg.Message = stringField(req.Config, "message")
	cfg.Text = stringField(req.Config, "text")
	cfg.Caption = stringField(req.Config, "caption")
	cfg.FileURL = stringField(req.Config, "file_url")

	return cfg
}

// extractAuthContext pulls "_auth_context" from a raw config map.
func extractAuthContext(raw map[string]interface{}) map[string]models.AuthData {
	v, ok := raw["_auth_context"]
	if !ok {
		return nil
	}
	// Re-marshal and unmarshal to convert map[string]interface{} → map[string]AuthData
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var result map[string]models.AuthData
	if err := json.Unmarshal(b, &result); err != nil {
		return nil
	}
	return result
}

func handleSetup(w http.ResponseWriter, r *http.Request) {
	var payload SetupPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "invalid JSON"})
		return
	}

	log.Printf("[Setup] id=%s workflow=%s capability=%s queue=%s", payload.ID, payload.WorkflowID, payload.CapabilityKey, payload.QueueName)

	if payload.QueueName == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "queue_name is required"})
		return
	}
	if payload.WorkflowID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "workflow_id is required"})
		return
	}

	config := buildActionConfig(payload)

	// Store the config, keyed by action ID.
	configs.Store(payload.ID, config)

	mu.Lock()
	defer mu.Unlock()

	// If a consumer for this workflow already exists, stop it before creating a new one.
	if existingConsumer, ok := consumers[payload.WorkflowID]; ok {
		log.Printf("[Setup] Stopping existing consumer for workflow %s", payload.WorkflowID)
		existingConsumer.Stop()
	}

	// Get RabbitMQ URL from environment or use default.
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}

	// The handler for the consumer needs a way to get the config.
	provider := &workflowConfigProvider{}

	// Create and start new consumer with a sequence number
	seq := atomic.AddUint64(&consumerCount, 1)
	log.Printf("[Setup] Consumer #%d assigned to workflow %s queue %s", seq, payload.WorkflowID, payload.QueueName)

	if telegramSvc == nil {
		telegramSvc = services.NewTelegramService()
	}
	if resultPublisher == nil {
		resultPublisher = worker.NewPublisher()
	}

	taskHandler := telegramSvc.HandleTaskRouter(provider, resultPublisher, seq)

	consumerTag := fmt.Sprintf("telegram-action-%s", payload.WorkflowID)
	consumer := worker.NewConsumer(rabbitmqURL, payload.QueueName, consumerTag, taskHandler)
	consumer.Start()

	consumers[payload.WorkflowID] = consumer

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"message":    "setup_complete",
		"queue_name": payload.QueueName,
		"seq":        seq,
	})
}

func handleRemove(w http.ResponseWriter, r *http.Request) {
	var payload RemovePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("[Remove] id=%s workflow=%s", payload.ID, payload.WorkflowID)

	// The key for consumers and configs is WorkflowID
	if payload.WorkflowID == "" {
		http.Error(w, "workflow_id is required", http.StatusBadRequest)
		return
	}

	// Stop and remove the consumer keyed by workflowID
	if consumer, exists := consumers[payload.WorkflowID]; exists {
		log.Printf("[Remove] Stopping consumer for workflow %s", payload.WorkflowID)
		consumer.Stop()
		delete(consumers, payload.WorkflowID)
		log.Printf("[Remove] Stopped and removed consumer for workflow %s", payload.WorkflowID)
	}

	// Remove the specific action config
	configs.Delete(payload.ID)
	log.Printf("[Remove] Removed config for action %s", payload.ID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "removed",
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	active := len(consumers)
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":           "ok",
		"message":          "service is healthy",
		"active_consumers": active,
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
	})
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[Main] Starting Telegram Action plugin")

	// Initialise Telegram service.
	telegramSvc = services.NewTelegramService()

	// Register with workflow_executor in the background with retry.
	regService := services.NewRegistrationService()
	go func() {
		for i := 0; i < 10; i++ {
			if err := regService.Register(); err == nil {
				return
			} else {
				log.Printf("[Main] Registration attempt %d failed: %v", i+1, err)
			}
			backoff := time.Duration(i+1) * 5 * time.Second
			log.Printf("[Main] Retrying registration in %v", backoff)
			time.Sleep(backoff)
		}
		log.Println("[Main] WARNING: Could not register with executor after 10 attempts")
	}()

	// HTTP routes.
	http.HandleFunc("/setup", handleSetup)
	http.HandleFunc("/remove", handleRemove)
	http.HandleFunc("/health", handleHealth)

	// PLUGIN_LISTEN_PORT is the internal port for Nginx proxying (set by start.sh).
	// Falls back to PLUGIN_PORT for standalone/local deployments.
	port := os.Getenv("PLUGIN_LISTEN_PORT")
	if port == "" {
		port = os.Getenv("PLUGIN_PORT")
	}
	if port == "" {
		port = "8057" // Default for telegram_action
	}

	log.Printf("[Main] Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Main] Server error: %v", err)
	}
}
