package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"x_action/models"
	"x_action/services"
	"x_action/worker"
)

var (
	consumers       = make(map[string]*worker.Consumer)
	configs         = &sync.Map{}
	mu              sync.Mutex
	xSvc            *services.XService
	resultPublisher *worker.Publisher
	consumerCount   uint64
)

type workflowConfigProvider struct{}

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
	if m == nil {
		return ""
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// buildActionConfig converts the raw setup payload into a typed ActionConfig
func buildActionConfig(req SetupPayload) models.ActionConfig {
	cfg := models.ActionConfig{
		CapabilityKey: req.CapabilityKey,
	}

	if authCtxRaw, ok := req.Config["_auth_context"]; ok {
		// Try direct typed map first
		if authCtx, ok := authCtxRaw.(map[string]models.AuthData); ok {
			cfg.AuthContext = authCtx
		} else {
			cfg.AuthContext = extractAuthContext(req.Config)
		}
	} else {
		cfg.AuthContext = extractAuthContext(req.Config)
	}

	rawCfg := make(map[string]interface{})
	for k, v := range req.Config {
		if k != "_auth_context" {
			rawCfg[k] = v
		}
	}
	cfg.RawConfig = rawCfg

	// Extract typed X-specific fields
	cfg.TweetText = stringField(req.Config, "tweet_text")
	cfg.ImageURL = stringField(req.Config, "image_url")

	return cfg
}

func extractAuthContext(raw map[string]interface{}) map[string]models.AuthData {
	v, ok := raw["_auth_context"]
	if !ok {
		return nil
	}
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
	configs.Store(payload.ID, config)

	mu.Lock()
	defer mu.Unlock()

	if existingConsumer, ok := consumers[payload.ID]; ok {
		log.Printf("[Setup] Stopping existing consumer for id %s", payload.ID)
		existingConsumer.Stop()
	}

	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}

	provider := &workflowConfigProvider{}

	seq := atomic.AddUint64(&consumerCount, 1)
	log.Printf("[Setup] Consumer #%d assigned to workflow %s queue %s", seq, payload.WorkflowID, payload.QueueName)

	if xSvc == nil {
		xSvc = services.NewXService()
	}
	if resultPublisher == nil {
		resultPublisher = worker.NewPublisher()
	}

	taskHandler := xSvc.HandleTaskRouter(provider, resultPublisher, seq, payload.ID, config)

	consumerTag := fmt.Sprintf("x-action-%s", payload.WorkflowID)
	consumer := worker.NewConsumer(rabbitmqURL, payload.QueueName, consumerTag, taskHandler)
	consumer.Start()

	consumers[payload.ID] = consumer

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
		log.Printf("[Remove] Error: invalid JSON: %v", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	id := payload.ID
	workflowID := payload.WorkflowID

	log.Printf("[Remove] Processing removal for id=%s workflow=%s", id, workflowID)

	mu.Lock()
	defer mu.Unlock()

	removedCount := 0
	if id != "" {
		if consumer, exists := consumers[id]; exists {
			log.Printf("[Remove] Stopping consumer for id %s", id)
			consumer.Stop()
			delete(consumers, id)
			configs.Delete(id)
			removedCount++
		}
	}

	if removedCount == 0 && id != "" {
		log.Printf("[Remove] Warning: No active consumer found for id %s", id)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"message":       "removed",
		"removed_count": removedCount,
	})
}

func handleValidate(w http.ResponseWriter, r *http.Request) {
	var payload SetupPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf("[Validate] Error: invalid JSON: %v", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	config := buildActionConfig(payload)

	val, ok := configs.Load(payload.ID)
	isDuplicate := false
	if ok {
		existingConfig := val.(models.ActionConfig)
		if reflect.DeepEqual(existingConfig.RawConfig, config.RawConfig) &&
			reflect.DeepEqual(existingConfig.AuthContext, config.AuthContext) &&
			existingConfig.CapabilityKey == config.CapabilityKey {
			isDuplicate = true
			log.Printf("[Validate] Duplicate detected for id=%s", payload.ID)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"is_duplicate": isDuplicate})
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

func main() {
	log.Println("[Main] Starting X Action plugin")

	if os.Getenv("X_CLIENT_ID") == "" || os.Getenv("X_CLIENT_SECRET") == "" {
		log.Println("[Main] WARNING: X_CLIENT_ID or X_CLIENT_SECRET not set. Token refresh may fail.")
	}

	xSvc = services.NewXService()

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

	prefix := "/x/action"
	http.HandleFunc(prefix+"/setup", handleSetup)
	http.HandleFunc(prefix+"/remove", handleRemove)
	http.HandleFunc(prefix+"/validate", handleValidate)
	http.HandleFunc(prefix+"/health", handleHealth)

	port := getPort()
	log.Printf("[Main] Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Main] Server error: %v", err)
	}
}
