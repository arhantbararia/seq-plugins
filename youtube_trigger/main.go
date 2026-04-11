package main

//agent pls complete and go through for any errors

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"youtube_trigger/models"
	"youtube_trigger/services"
	"youtube_trigger/worker"
)

// ── Global state ─────────────────────────────────────────────────────────────

var (
	publisher *worker.Publisher
	pollers   = make(map[string]*services.Poller)
	mu        sync.Mutex
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

	mu.Lock()
	defer mu.Unlock()

	if existing, exists := pollers[id]; exists {
		log.Printf("[Setup] Stopping existing poller for id=%s before re-setup", id)
		existing.Stop()
	}

	var auth models.AuthData
	if authIface, ok := payload.Config["_auth_context"]; ok {
		// Re-marshal and unmarshal to convert map[string]interface{} → map[string]models.AuthData
		b, _ := json.Marshal(authIface)
		var authMap map[string]models.AuthData
		if err := json.Unmarshal(b, &authMap); err == nil {
			log.Printf("[Setup] Auth context contains providers: %v", getMapKeys(authMap))
			// Try specific providers for YouTube (Google)
			targets := []string{"google", "YouTube", "youtube", "google-oauth2", "google-oauth"}
			for _, target := range targets {
				if a, ok := authMap[target]; ok && a.AccessToken != "" {
					auth = a
					auth.Provider = target
					log.Printf("[Setup] Found %s auth for id=%s", target, id)
					break
				}
			}
		}
	}

	if auth.AccessToken == "" {
		log.Printf("[Setup] WARNING: No access_token found in _auth_context for id=%s", id)
	}

	var apiKey string
	// Try to find api_key in config directly
	if val, ok := payload.Config["api_key"].(string); ok && val != "" {
		apiKey = val
	} else if val, ok := payload.Config["apiKey"].(string); ok && val != "" {
		apiKey = val
	}

	poller := services.NewPoller(id, payload.WorkflowID, payload.CapabilityKey, payload.Config, auth, apiKey, publisher)
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
		port = "8085"
	}

	log.Printf("[Main] Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Main] Server error: %v", err)
	}
}
