package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"datetime_trigger/models"
	"datetime_trigger/services"
	"datetime_trigger/worker"
)

// ── Global state ─────────────────────────────────────────────────────────────

var (
	publisher  *worker.Publisher
	schedulers = make(map[string]*services.Scheduler)
	// configs stores trigger configurations, keyed by trigger instance ID.
	configs = &sync.Map{}
	mu      sync.Mutex

	schedulerCount uint64
)

// ── Setup payload (sent by workflow_executor) ─────────────────────────────────

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

// buildTriggerConfig extracts typed fields from the raw config map.
// It returns a fresh models.TriggerConfig with an isolated AuthContext.
func buildTriggerConfig(req SetupPayload) models.TriggerConfig {
	cfg := models.TriggerConfig{
		CapabilityKey: req.CapabilityKey,
		AuthContext:   map[string]models.AuthData{}, // Date & Time needs no auth
	}

	// Extract optional scheduling fields from config map.
	if v, ok := req.Config["scheduled_at"].(string); ok {
		cfg.ScheduledAt = v
	}
	if v, ok := req.Config["day_of_week"].(string); ok {
		cfg.DayOfWeek = v
	}
	if v, ok := req.Config["day_of_month"]; ok {
		switch d := v.(type) {
		case float64:
			cfg.DayOfMonth = int(d)
		case int:
			cfg.DayOfMonth = d
		}
	}

	return cfg
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func handleSetup(w http.ResponseWriter, r *http.Request) {
	var payload SetupPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("[Setup] id=%s workflow=%s capability=%s", payload.ID, payload.WorkflowID, payload.CapabilityKey)

	config := buildTriggerConfig(payload)

	// Store the config, keyed by trigger instance ID.
	configs.Store(payload.ID, config)

	mu.Lock()
	defer mu.Unlock()

	// Stop existing scheduler if this is a re-setup (update).
	if existing, exists := schedulers[payload.ID]; exists {
		log.Printf("[Setup] Stopping existing scheduler for id=%s before re-setup", payload.ID)
		existing.Stop()
	}

	// Create and start the new scheduler.
	seq := atomic.AddUint64(&schedulerCount, 1)
	sched := services.NewScheduler(payload.ID, payload.WorkflowID, config, seq, publisher)
	schedulers[payload.ID] = sched
	sched.Start()

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

	log.Printf("[Remove] id=%s workflow=%s", payload.ID, payload.WorkflowID)

	mu.Lock()
	defer mu.Unlock()

	if sched, exists := schedulers[payload.ID]; exists {
		sched.Stop()
		delete(schedulers, payload.ID)
		configs.Delete(payload.ID)
		log.Printf("[Remove] Stopped and removed scheduler for id=%s", payload.ID)
	} else {
		log.Printf("[Remove] No scheduler found for id=%s", payload.ID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"removed"}`))
}

func handleValidate(w http.ResponseWriter, r *http.Request) {
	var payload SetupPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf("[Validate] Error: invalid JSON: %v", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	config := buildTriggerConfig(payload)
	
	// Check if this ID already exists and has identical config
	val, ok := configs.Load(payload.ID)
	isDuplicate := false
	if ok {
		existingConfig := val.(models.TriggerConfig)
		// Compare key parts of the config
		if reflect.DeepEqual(existingConfig.AuthContext, config.AuthContext) &&
		   existingConfig.CapabilityKey == config.CapabilityKey &&
		   existingConfig.ScheduledAt == config.ScheduledAt &&
		   existingConfig.DayOfWeek == config.DayOfWeek &&
		   existingConfig.DayOfMonth == config.DayOfMonth {
			isDuplicate = true
			log.Printf("[Validate] Duplicate detected for id=%s", payload.ID)
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
	active := len(schedulers)
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "ok",
		"message":         "setup_complete",
		"active_triggers": active,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
	})
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	log.Println("[Main] Starting Date & Time Trigger plugin")

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
	http.HandleFunc("/setup", handleSetup)
	http.HandleFunc("/remove", handleRemove)
	http.HandleFunc("/validate", handleValidate)
	http.HandleFunc("/health", handleHealth)

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
