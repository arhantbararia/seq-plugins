package main

//agent pls complete and go through for any errors

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

	"youtube_trigger/models"
	"youtube_trigger/services"
	"youtube_trigger/worker"
)

// ── Global state ─────────────────────────────────────────────────────────────

var (
	publisher *worker.Publisher
	pollers   = make(map[string]*services.Poller)
	// configs stores trigger configurations, keyed by trigger instance ID.
	configs = &sync.Map{}
	mu      sync.Mutex

	pollerCount uint64
)

// ── Setup payload (sent by workflow_executor) ─────────────────────────────────

type SetupPayload struct {
	ID            string                 `json:"id"`
	TriggerID     string                 `json:"trigger_id"`
	WorkflowID    string                 `json:"workflow_id"`
	QueueName     string                 `json:"queue_name"`
	CapabilityKey string                 `json:"capability_key"`
	Config        map[string]interface{} `json:"config"`
}

type RemovePayload struct {
	ID         string `json:"id"`
	TriggerID  string `json:"trigger_id"`
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

// extractAuthContext pulls "_auth_context" from a raw config map.
func extractAuthContext(raw map[string]interface{}) map[string]models.AuthData {
	v, ok := raw["_auth_context"]
	if !ok {
		return nil
	}
	// Re-marshal and unmarshal to convert map[string]interface{} → map[string]models.AuthData
	b, _ := json.Marshal(v)
	var result map[string]models.AuthData
	if err := json.Unmarshal(b, &result); err != nil {
		return nil
	}
	return result
}

// buildTriggerConfig extracts typed fields from the raw config map.
func buildTriggerConfig(req SetupPayload) models.TriggerConfig {
	return models.TriggerConfig{
		CapabilityKey: req.CapabilityKey,
		AuthContext:   extractAuthContext(req.Config),
		SearchQuery:   stringField(req.Config, "q"),
		ChannelNameOrID: stringField(req.Config, "channel"),
	}
}

// pollerConfigUpdater implements services.ConfigUpdater to allow pollers to persist refreshed tokens.
type pollerConfigUpdater struct{}

func (u *pollerConfigUpdater) UpdateAuth(id string, auth map[string]models.AuthData) error {
	val, ok := configs.Load(id)
	if !ok {
		return fmt.Errorf("config not found for trigger %s", id)
	}
	cfg := val.(models.TriggerConfig)
	cfg.AuthContext = auth
	configs.Store(id, cfg)
	log.Printf("[Updater] Persisted refreshed tokens for trigger %s", id)
	return nil
}

// copyMap creates a shallow copy of a map to ensure isolation.
func copyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func handleSetup(w http.ResponseWriter, r *http.Request) {
	var payload SetupPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	id := payload.ID
	if id == "" {
		id = payload.TriggerID
	}

	log.Printf("[Setup] id=%s workflow=%s capability=%s", id, payload.WorkflowID, payload.CapabilityKey)

	// Build a fresh config. AuthContext is unmarshaled into a new map reference here.
	config := buildTriggerConfig(payload)
	// Create a copy of the raw config to prevent pointer/reference crosstalk between instances.
	rawConf := copyMap(payload.Config)

	// Store the config, keyed by trigger instance ID.
	configs.Store(id, config)

	mu.Lock()
	defer mu.Unlock()

	if existing, exists := pollers[id]; exists {
		log.Printf("[Setup] Stopping existing poller for id=%s before re-setup", id)
		existing.Stop()
	}

	seq := atomic.AddUint64(&pollerCount, 1)
	updater := &pollerConfigUpdater{}
	poller := services.NewPoller(id, payload.WorkflowID, config, rawConf, seq, publisher, updater)
	pollers[id] = poller
	poller.Start()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"setup_complete"}`))
}

func getMapKeys(m map[string]models.AuthData) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func handleRemove(w http.ResponseWriter, r *http.Request) {
	var payload RemovePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	id := payload.ID
	if id == "" {
		id = payload.TriggerID
	}
	workflowID := payload.WorkflowID

	log.Printf("[Remove] id=%s workflow=%s", id, workflowID)

	mu.Lock()
	defer mu.Unlock()

	removedCount := 0

	// 1. Try to find by ID/TriggerID
	if id != "" {
		if poller, exists := pollers[id]; exists {
			poller.Stop()
			delete(pollers, id)
			configs.Delete(id)
			removedCount++
			log.Printf("[Remove] Stopped and removed poller id=%s", id)
		}
	}

	// 2. Fallback: Search all pollers by WorkflowID if no specific ID matched
	// (Important because workflow_executor might send workflowID as "id" during bulk removal)
	if workflowID != "" {
		for pID, poller := range pollers {
			if poller.WorkflowID == workflowID {
				poller.Stop()
				delete(pollers, pID)
				configs.Delete(pID)
				removedCount++
				log.Printf("[Remove] Stopped and removed poller id=%s (matched by workflow=%s)", pID, workflowID)
			}
		}
	}

	if removedCount == 0 {
		log.Printf("[Remove] No active pollers found for removal (id=%s workflow=%s)", id, workflowID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"removed","removed_count":` + fmt.Sprintf("%d", removedCount) + `}`))
}

func handleValidate(w http.ResponseWriter, r *http.Request) {
	var payload SetupPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf("[Validate] Error: invalid JSON: %v", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	id := payload.ID
	if id == "" {
		id = payload.TriggerID
	}

	config := buildTriggerConfig(payload)
	
	val, ok := configs.Load(id)
	isDuplicate := false
	if ok {
		existingConfig := val.(models.TriggerConfig)
		// Compare key parts of the config
		if reflect.DeepEqual(existingConfig.AuthContext, config.AuthContext) &&
		   existingConfig.CapabilityKey == config.CapabilityKey &&
		   existingConfig.SearchQuery == config.SearchQuery &&
		   existingConfig.ChannelNameOrID == config.ChannelNameOrID {
			isDuplicate = true
			log.Printf("[Validate] Duplicate detected for id=%s", id)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"is_duplicate": isDuplicate,
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	active := len(pollers)
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "ok",
		"message":         "healthy",
		"active_triggers": active,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
	})
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[Main] Starting YouTube Trigger plugin")

	// Initialise RabbitMQ publisher.
	publisher = worker.NewPublisher()

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
	prefix := os.Getenv("PLUGIN_ROUTE_PREFIX")
	http.HandleFunc(prefix+"/setup", handleSetup)
	http.HandleFunc(prefix+"/remove", handleRemove)
	http.HandleFunc(prefix+"/validate", handleValidate)
	http.HandleFunc(prefix+"/health", handleHealth)

	// PLUGIN_LISTEN_PORT is the internal port for Nginx proxying (set by start.sh).
	// Falls back to PLUGIN_PORT for standalone/local deployments.
	port := os.Getenv("PLUGIN_LISTEN_PORT")
	if port == "" {
		port = os.Getenv("PLUGIN_PORT")
	}
	if port == "" {
		port = "8085"
	}

	log.Printf("[Main] Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Main] Server error: %v", err)
	}
}
