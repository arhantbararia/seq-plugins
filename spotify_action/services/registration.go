package services

//agent pls complete and check for any errors and smooth operation
import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"spotify_action/models"
)

// RegistrationService handles self-registration with the workflow_executor.
type RegistrationService struct{}

func NewRegistrationService() *RegistrationService {
	return &RegistrationService{}
}

func (s *RegistrationService) Register() error {
	executorURL := os.Getenv("WORKFLOW_EXECUTOR_URL")
	if executorURL == "" {
		executorURL = "http://localhost:8082"
	}
	host := os.Getenv("PLUGIN_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PLUGIN_PORT")
	if port == "" {
		port = "8086"
	}
	outputSchema := map[string]interface{}{
		"result": "object", // Spotify API response
	}
	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-spotify-action"
	}

	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "Spotify Action",
		ContainerType:         "action",
		PluginProviderService: "Spotify",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":  "/spotify/setup",
			"remove": "/spotify/remove",
			"health": "/spotify/health",
		},
		AuthTypes: []string{"OAUTH2"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "spotify_add_to_queue",
				Name:          "Add track to playback queue",
				Description:   "Adds a track to the user's playback queue. TrackID: eg.https://open.spotify.com/track/5...A?si=2...d , Track Query: eg. Bohemian Rhapsody",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"track_id":    "string",
					"track_query": "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "spotify_add_to_playlist_by_id",
				Name:          "Add track to a playlist by TrackID",
				Description:   "Adds a specific track to a playlist using their IDs. TrackID: eg.https://open.spotify.com/track/5...A?si=2...d , PlaylistID: eg. https://open.spotify.com/playlist/6...5a?si=6ee..7c ",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"track_id":    "string",
					"playlist_id": "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "spotify_save_track",
				Name:          "Save a track",
				Description:   "Saves a track to the current user's 'Your Music' library. TrackID: eg.https://open.spotify.com/track/5...A?si=2...d , Track Query: eg. Bohemian Rhapsody",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"track_id":    "string",
					"track_query": "string", // search fallback
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "spotify_follow_playlist",
				Name:          "Follow a playlist",
				Description:   "Add the current user as a follower of a playlist. PlaylistID: eg. https://open.spotify.com/playlist/6...5a?si=6ee..7c ",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"playlist_id": "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "spotify_add_to_playlist",
				Name:          "Add track to a playlist",
				Description:   "Adds a track (by query or ID) to a playlist. TrackID: eg.https://open.spotify.com/track/5...A?si=2...d ,  Track Query: eg. Bohemian Rhapsody, PlaylistID: eg. https://open.spotify.com/playlist/6...5a?si=6ee..7c ",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"track_id":    "string", // optional if query provided
					"track_query": "string", // fallback search
					"playlist_id": "string",
				},
				OutputSchema: outputSchema,
			},
		},
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal registration: %w", err)
	}

	resp, err := http.Post(executorURL+"/register", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("post /register: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("executor returned status %d", resp.StatusCode)
	}

	log.Printf("[Registration] Successfully registered with executor at %s (host=%s port=%s)",
		executorURL, host, port)
	return nil
}
