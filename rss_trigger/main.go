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

	"rss_trigger/models"
	"rss_trigger/services"
	"rss_trigger/worker"
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

// buildTriggerConfig extracts typed fields from the raw config map.
func buildTriggerConfig(req SetupPayload) models.TriggerConfig {
	return models.TriggerConfig{
		CapabilityKey: req.CapabilityKey,
		FeedURL:       stringField(req.Config, "feed_url"),
		Keyword:       stringField(req.Config, "keyword"),
	}
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

	config := buildTriggerConfig(payload)

	// Store the config, keyed by trigger instance ID.
	configs.Store(id, config)

	mu.Lock()
	defer mu.Unlock()

	if existing, exists := pollers[id]; exists {
		log.Printf("[Setup] Stopping existing poller for id=%s before re-setup", id)
		existing.Stop()
	}

	seq := atomic.AddUint64(&pollerCount, 1)
	poller := services.NewPoller(id, payload.WorkflowID, config, seq, publisher)
	pollers[id] = poller
	poller.Start()

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

	// 2. Fallback: Search all pollers by WorkflowID
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

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[Main] Starting RSS Trigger plugin")

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
	prefix := "/rss/trigger"
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
