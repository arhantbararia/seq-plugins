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

	"instagram_trigger/models"
	"instagram_trigger/services"
	"instagram_trigger/worker"
)

// Global state
var (
	publisher *worker.Publisher
	pollers   = make(map[string]*services.Poller)
	configs   = &sync.Map{}
	mu        sync.Mutex

	pollerCount uint64
)

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

// Convert _auth_context (if present) into typed map
func parseAuthContext(cfg map[string]interface{}) map[string]models.AuthData {
	if cfg == nil {
		return nil
	}
	if v, ok := cfg["_auth_context"]; ok {
		// marshal/unmarshal to convert types
		b, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		var out map[string]models.AuthData
		if err := json.Unmarshal(b, &out); err != nil {
			return nil
		}
		return out
	}
	return nil
}

func buildTriggerConfig(req SetupPayload) models.TriggerConfig {
	return models.TriggerConfig{
		CapabilityKey: req.CapabilityKey,
		Hashtag:       stringField(req.Config, "hashtag"),
		AuthContext:   parseAuthContext(req.Config),
	}
}

// updater implements services.ConfigUpdater and persists refreshed auth
type updater struct{}

func (u *updater) UpdateAuth(id string, auth map[string]models.AuthData) error {
	if id == "" {
		return fmt.Errorf("empty id")
	}
	val, ok := configs.Load(id)
	if !ok {
		return fmt.Errorf("config for id not found")
	}
	cfg, ok := val.(models.TriggerConfig)
	if !ok {
		return fmt.Errorf("invalid config type")
	}
	cfg.AuthContext = auth
	configs.Store(id, cfg)
	return nil
}

// Handlers
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

	config := buildTriggerConfig(payload)

	configs.Store(id, config)

	mu.Lock()
	defer mu.Unlock()

	if existing, exists := pollers[id]; exists {
		log.Printf("[Setup] Stopping existing poller for id=%s before re-setup", id)
		existing.Stop()
	}

	seq := atomic.AddUint64(&pollerCount, 1)
	p := services.NewPoller(id, payload.WorkflowID, config, seq, publisher, &updater{})
	pollers[id] = p
	p.Start()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"setup_complete"}`))
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

	if id != "" {
		if poller, exists := pollers[id]; exists {
			poller.Stop()
			delete(pollers, id)
			configs.Delete(id)
			removedCount++
			log.Printf("[Remove] Stopped and removed poller id=%s", id)
		}
	}

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
		if reflect.DeepEqual(existingConfig, config) {
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
	log.Println("[Main] Starting Instagram Trigger plugin")

	publisher = worker.NewPublisher()

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

	prefix := "/instagram/trigger"
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
