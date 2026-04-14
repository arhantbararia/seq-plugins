package main

//agent pls complete and go through for any errors
import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"spotify_action/models"
	"spotify_action/services"
	"spotify_action/worker"
	"strings"
	"sync"
	"sync/atomic"

	"time"
)
// Global state
var (
	// consumers map stores active consumers, keyed by subscription ID (Payload.ID).
	consumers = make(map[string]*worker.Consumer)
	// configs stores action configurations, keyed by action ID.
	configs = &sync.Map{}
	// mu protects the consumers map.
	mu sync.Mutex

	// spotifySvc is the service for interacting with the Spotify API.
	// This is initialized in main().
	spotifySvc *services.SpotifyService

	// resultPublisher handles publishing ActionResults to RabbitMQ
	resultPublisher *worker.Publisher

	consumerCount uint64
)

// workflowConfigProvider implements the services.ConfigProvider interface,
// allowing RabbitMQ handlers to retrieve configuration for a workflow.
type workflowConfigProvider struct{}

// GetConfig retrieves the ActionConfig for a given ID from the global store.
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

func extractTrackID(m map[string]interface{}, key string) string {
	val := stringField(m, key)
	if val == "" {
		return ""
	}

	// Handle Spotify URL: https://open.spotify.com/track/69ivV09HWiq7T7nIkFe3nq?si=...
	if strings.Contains(val, "open.spotify.com/track/") {
		parts := strings.Split(val, "track/")
		if len(parts) > 1 {
			id := parts[1]
			// Strip query parameters if present
			if idx := strings.Index(id, "?"); idx != -1 {
				id = id[:idx]
			}
			return strings.TrimSpace(id)
		}
	}

	return val
}

func extractPlaylistID(m map[string]interface{}, key string) string {
	val := stringField(m, key)
	if val == "" {
		return ""
	}

	// Handle Spotify URL: https://open.spotify.com/playlist/6r64LmFqWMygcZSIApMY5a?si=7538b
	if strings.Contains(val, "open.spotify.com/track/") {
		parts := strings.Split(val, "playlist/")
		if len(parts) > 1 {
			id := parts[1]
			// Strip query parameters if present
			if idx := strings.Index(id, "?"); idx != -1 {
				id = id[:idx]
			}
			return strings.TrimSpace(id)
		}
	}

	return val
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
	// This allows {{trigger.payload.X}} in any config field to be resolved.
	rawCfg := make(map[string]interface{})
	for k, v := range req.Config {
		if k != "_auth_context" {
			rawCfg[k] = v
		}
	}
	cfg.RawConfig = rawCfg

	// Extract string fields from config map
	cfg.TrackID = extractTrackID(req.Config, "track_id")
	cfg.TrackQuery = stringField(req.Config, "track_query")
	cfg.PlaylistID = extractPlaylistID(req.Config, "playlist_id")

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

	// If a consumer for this subscription already exists, stop it before creating a new one.
	if existingConsumer, ok := consumers[payload.ID]; ok {
		log.Printf("[Setup] Stopping existing consumer for id %s", payload.ID)
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

	if spotifySvc == nil {
		spotifySvc = services.NewSpotifyService()
	}
	if resultPublisher == nil {
		resultPublisher = worker.NewPublisher()
	}

	taskHandler := spotifySvc.HandleTaskRouter(provider, resultPublisher, seq)

	consumerTag := fmt.Sprintf("spotify-action-%s", payload.WorkflowID)
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

	// 1. Try to find by ID
	if id != "" {
		if consumer, exists := consumers[id]; exists {
			log.Printf("[Remove] Stopping consumer for id %s", id)
			consumer.Stop()
			delete(consumers, id)
			configs.Delete(id)
			removedCount++
		}
	}

	// 2. Fallback: Search all consumers by WorkflowID
	// (Note: This would require consumer to store its WorkflowID, we skip for now to avoid model changes)

	if removedCount == 0 && id != "" {
		log.Printf("[Remove] Warning: No active consumer found for id %s", id)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "removed",
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
	
	// Check if this ID already exists and has identical config
	val, ok := configs.Load(payload.ID)
	isDuplicate := false
	if ok {
		existingConfig := val.(models.ActionConfig)
		// Compare key parts of the config
		if reflect.DeepEqual(existingConfig.RawConfig, config.RawConfig) && 
		   reflect.DeepEqual(existingConfig.AuthContext, config.AuthContext) &&
		   existingConfig.CapabilityKey == config.CapabilityKey {
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
	log.Println("[Main] Starting Spotify Action plugin")

	// Verify environment variables for OAuth refresh
	if os.Getenv("SPOTIFY_CLIENT_ID") == "" || os.Getenv("SPOTIFY_CLIENT_SECRET") == "" {
		log.Println("[Main] WARNING: SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not set. Token refresh will fail.")
	}
	spotifySvc = services.NewSpotifyService()

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
		port = "8086"
	}

	log.Printf("[Main] Listening on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("[Main] Server error: %v", err)
	}
}
