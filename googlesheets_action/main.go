package main

//agent pls complete and go through for any errors
import (
	"encoding/json"
	"fmt"
	"googlesheets_action/models"
	"googlesheets_action/services"
	"googlesheets_action/worker"
	"log"
	"net/http"
	"os"
	"sync"

	"time"
)

// Global state
var (
	// consumers map stores active consumers, keyed by workflow ID.
	consumers = make(map[string]*worker.Consumer)
	// configs stores action configurations, keyed by workflow ID.
	// Using sync.Map for safe concurrent access from HTTP handlers and RabbitMQ handlers.
	configs = &sync.Map{}
	// mu protects the consumers map.
	mu sync.Mutex
	// googleSheetsSvc is the service for interacting with the Google Sheets API.
	// This is initialized in main().
	googleSheetsSvc *services.GoogleSheetsService
	// resultPublisher handles publishing ActionResults to RabbitMQ
	resultPublisher *worker.Publisher
)

// workflowConfigProvider implements the services.ConfigProvider interface,
// allowing RabbitMQ handlers to retrieve configuration for a workflow.
type workflowConfigProvider struct{}

// GetConfig retrieves the ActionConfig for a given workflow ID from the global store.
func (p *workflowConfigProvider) GetConfig(workflowID string) (models.ActionConfig, error) {
	val, ok := configs.Load(workflowID)
	if !ok {
		return models.ActionConfig{}, fmt.Errorf("config not found for workflow %s", workflowID)
	}
	cfg, ok := val.(models.ActionConfig)
	if !ok {
		return models.ActionConfig{}, fmt.Errorf("invalid config type in store for workflow %s", workflowID)
	}
	return cfg, nil
}

func (p *workflowConfigProvider) UpdateAuth(workflowID string, auth map[string]models.AuthData) error {
	val, ok := configs.Load(workflowID)
	if !ok {
		return fmt.Errorf("config not found for workflow %s", workflowID)
	}
	cfg, ok := val.(models.ActionConfig)
	if !ok {
		return fmt.Errorf("invalid config type in store for workflow %s", workflowID)
	}
	cfg.AuthContext = auth
	configs.Store(workflowID, cfg)
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
	cfg.SpreadsheetID = stringField(req.Config, "spreadsheet_id")
	cfg.Worksheet = stringField(req.Config, "worksheet")
	cfg.CellCoordinates = stringField(req.Config, "cell_coordinates")
	cfg.Value = stringField(req.Config, "value")
	cfg.RowValues = stringField(req.Config, "row_values")

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

	// Store the config, keyed by workflow ID. This is thread-safe.
	configs.Store(payload.WorkflowID, config)

	// The rest of the logic modifies the shared consumers map and needs protection.
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

	// The googleSheetsSvc is a global, assumed to be initialized in main().
	if googleSheetsSvc == nil {
		googleSheetsSvc = services.NewGoogleSheetsService()
		log.Println("[Setup] Warning: googleSheetsSvc was not initialized, creating new instance now.")
	}

	if resultPublisher == nil {
		resultPublisher = worker.NewPublisher()
		log.Println("[Setup] Warning: resultPublisher was not initialized, creating new instance now.")
	}

	taskHandler := googleSheetsSvc.HandleTaskRouter(provider, resultPublisher)

	// Create a new consumer for the specified queue.
	consumerTag := fmt.Sprintf("googlesheets-action-%s", payload.WorkflowID)
	consumer := worker.NewConsumer(rabbitmqURL, payload.QueueName, consumerTag, taskHandler)

	// Start the consumer. It runs in its own goroutine and handles reconnections.
	consumer.Start()

	// Store the new consumer, replacing the old one.
	consumers[payload.WorkflowID] = consumer

	log.Printf("[Setup] Started consumer for queue '%s'", payload.QueueName)

	// Respond with success as per the spec.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"message":    "setup_complete",
		"queue_name": payload.QueueName,
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

	mu.Lock()
	defer mu.Unlock()

	// Stop and remove the consumer
	if consumer, exists := consumers[payload.WorkflowID]; exists {
		log.Printf("[Remove] Stopping consumer for workflow %s", payload.WorkflowID)
		consumer.Stop()
		delete(consumers, payload.WorkflowID)
		log.Printf("[Remove] Stopped and removed consumer for workflow %s", payload.WorkflowID)
	} else {
		log.Printf("[Remove] No consumer found for workflow %s", payload.WorkflowID)
	}

	// Remove the config
	configs.Delete(payload.WorkflowID)
	log.Printf("[Remove] Removed config for workflow %s", payload.WorkflowID)

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

func getPort() string {
	if port := os.Getenv("PLUGIN_LISTEN_PORT"); port != "" {
		return port
	}
	if port := os.Getenv("PLUGIN_PORT"); port != "" {
		return port
	}
	return "8080"
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[Main] Starting GoogleSheets Action plugin")

	// Initialise GoogleSheets service.
	googleSheetsSvc = services.NewGoogleSheetsService()

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
	prefix := "/googlesheets/action"
	http.HandleFunc(prefix+"/setup", handleSetup)
	http.HandleFunc(prefix+"/remove", handleRemove)
	http.HandleFunc(prefix+"/health", handleHealth)

	port := getPort()

	log.Printf("[Main] Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Main] Server error: %v", err)
	}
}
