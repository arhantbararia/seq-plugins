package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"spotify_action/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

const spotifyAPIBase = "https://api.spotify.com/v1"

type SpotifyService struct {
	httpClient *http.Client
}

func NewSpotifyService() *SpotifyService {
	return &SpotifyService{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// ConfigProvider defines an interface to retrieve an ActionConfig by ID
type ConfigProvider interface {
	GetConfig(id string) (models.ActionConfig, error)
	UpdateAuth(id string, auth map[string]models.AuthData) error
}

// PublisherProvider defines an interface to publish ActionResults
type PublisherProvider interface {
	Publish(workflowID string, result models.ActionResult) error
}

func getAuth(ctx map[string]models.AuthData) models.AuthData {
	for _, a := range ctx {
		return a
	}
	return models.AuthData{}
}

// templatePattern matches {{trigger.payload.field_name}} patterns.
var templatePattern = regexp.MustCompile(`\{\{trigger\.payload\.(\w+)\}\}`)

// resolveTemplates replaces all {{trigger.payload.X}} templates in the config's
// string fields with corresponding values from the trigger event payload.
// This is the core of the "ingredients" feature — any trigger output can flow
// into any action config field.
func resolveTemplates(cfg *models.ActionConfig, payload map[string]interface{}) {
	if payload == nil || cfg.RawConfig == nil {
		return
	}

	// Deep-copy RawConfig so we don't mutate the original stored config.
	// Maps are reference types in Go — without this copy, resolving templates
	// for one event would corrupt the stored templates for future events.
	resolved := make(map[string]interface{}, len(cfg.RawConfig))
	for k, v := range cfg.RawConfig {
		resolved[k] = v
	}
	cfg.RawConfig = resolved

	// resolveString replaces all {{trigger.payload.X}} in a single string
	resolveString := func(s string) string {
		if !strings.Contains(s, "{{trigger.payload.") {
			return s
		}
		return templatePattern.ReplaceAllStringFunc(s, func(match string) string {
			submatches := templatePattern.FindStringSubmatch(match)
			if len(submatches) < 2 {
				return match // no capture, leave as-is
			}
			key := submatches[1] // e.g. "title", "description"
			if val, ok := payload[key]; ok {
				return fmt.Sprintf("%v", val) // convert any type to string
			}
			log.Printf("[resolveTemplates] WARNING: payload missing key '%s', leaving template unreplaced", key)
			return match // key not in payload, leave template as-is
		})
	}

	// Resolve templates in raw config and re-extract typed fields
	for k, v := range cfg.RawConfig {
		if s, ok := v.(string); ok {
			cfg.RawConfig[k] = resolveString(s)
		}
	}

	// Re-extract typed fields from resolved raw config
	if v, ok := cfg.RawConfig["track_id"].(string); ok {
		cfg.TrackID = v
	}
	if v, ok := cfg.RawConfig["track_query"].(string); ok {
		cfg.TrackQuery = v
	}
	if v, ok := cfg.RawConfig["playlist_id"].(string); ok {
		cfg.PlaylistID = v
	}
}

func publishResult(publisher PublisherProvider, task models.ActionTask, resultOutput map[string]interface{}, elapsedMs int64, procErr error) {
	if publisher == nil {
		return
	}
	actionResult := models.ActionResult{
		TaskID:       task.ID,
		WorkflowID:   task.WorkflowID,
		Timestamp:    time.Now().UTC(),
		ResponseTime: elapsedMs,
	}

	if procErr != nil {
		actionResult.Success = false
		actionResult.Status = "error"
		actionResult.Error = procErr.Error()
	} else {
		actionResult.Success = true
		actionResult.Status = "success"
		actionResult.Output = resultOutput
		actionResult.RetryCount = 0
	}

	if pubErr := publisher.Publish(task.WorkflowID, actionResult); pubErr != nil {
		log.Printf("Failed to publish action result: %v", pubErr)
	}
}

// HandleTaskRouter dynamically routes to the specific capability method based on CapabilityKey.
// It uses a captured ActionConfig (assigned at setup) to ensure the consumer works
// in its own independent scope, ignoring any randomized IDs sent by triggers.
func (s *SpotifyService) HandleTaskRouter(cfgProvider ConfigProvider, publisher PublisherProvider, seq uint64, instanceID string, initialCfg models.ActionConfig) func(amqp.Delivery) {
	// Each consumer instance keeps its own "live" copy of the config (e.g. for auth refreshes)
	currentCfg := initialCfg

	return func(d amqp.Delivery) {
		var task models.ActionTask
		if err := json.Unmarshal(d.Body, &task); err != nil {
			log.Printf("[Consumer #%d] Error unmarshaling task: %v", seq, err)
			d.Nack(false, false)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Received task: %s", seq, task.WorkflowID, instanceID, task.CapabilityKey)

		// Resolve {{trigger.payload.X}} templates in a COPY of the current config 
		// to avoid mutating the base state for future tasks.
		taskCfg := currentCfg
		resolveTemplates(&taskCfg, task.Payload)

		log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Resolved config: track_query=%q, track_id=%q",
			seq, task.WorkflowID, instanceID, taskCfg.TrackQuery, taskCfg.TrackID)

		auth, err := s.GetValidAuth(instanceID, cfgProvider, &currentCfg)
		if err != nil {
			log.Printf("Error ensuring valid auth: %v", err)
			d.Nack(false, false)
			return
		}

		capability := taskCfg.CapabilityKey

		var procErr error
		var resultOutput map[string]interface{}
		var elapsedMs int64

		switch capability {
		case "spotify_add_to_queue":
			resultOutput, elapsedMs, procErr = s.AddToQueue(auth, taskCfg)
		case "spotify_add_to_playlist_by_id":
			resultOutput, elapsedMs, procErr = s.AddToPlaylistByID(auth, taskCfg)
		case "spotify_save_track":
			resultOutput, elapsedMs, procErr = s.SaveTrack(auth, taskCfg)
		case "spotify_follow_playlist":
			resultOutput, elapsedMs, procErr = s.FollowPlaylist(auth, taskCfg)
		case "spotify_add_to_playlist":
			resultOutput, elapsedMs, procErr = s.AddToPlaylist(auth, taskCfg)
		default:
			log.Printf("Unknown capability key: %s", capability)
			d.Nack(false, false)
			return
		}

		// Retry once on 401 Unauthorized if not already refreshed
		if procErr != nil && strings.Contains(procErr.Error(), "401") {
			log.Printf("[SpotifyService] Action failed with 401, trying immediate refresh and retry for instance %s", instanceID)
			newAuth, refreshErr := s.RefreshAccessToken(auth)
			if refreshErr == nil {
				// Update local configuration state
				for k := range currentCfg.AuthContext {
					currentCfg.AuthContext[k] = newAuth
					break
				}
				_ = cfgProvider.UpdateAuth(instanceID, currentCfg.AuthContext)

				// Retry the action with new auth and the resolved task config
				switch capability {
				case "spotify_add_to_queue":
					resultOutput, elapsedMs, procErr = s.AddToQueue(newAuth, taskCfg)
				case "spotify_add_to_playlist_by_id":
					resultOutput, elapsedMs, procErr = s.AddToPlaylistByID(newAuth, taskCfg)
				case "spotify_save_track":
					resultOutput, elapsedMs, procErr = s.SaveTrack(newAuth, taskCfg)
				case "spotify_follow_playlist":
					resultOutput, elapsedMs, procErr = s.FollowPlaylist(newAuth, taskCfg)
				case "spotify_add_to_playlist":
					resultOutput, elapsedMs, procErr = s.AddToPlaylist(newAuth, taskCfg)
				}
			} else {
				log.Printf("[SpotifyService] Refresh failed during retry: %v", refreshErr)
			}
		}

		publishResult(publisher, task, resultOutput, elapsedMs, procErr)

		if procErr != nil {
			log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Error processing capability %s: %v", seq, task.WorkflowID, instanceID, capability, procErr)
			d.Nack(false, true)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Successfully processed %s", seq, task.WorkflowID, instanceID, capability)
		d.Ack(false)
	}
}

// GetValidAuth checks if the token is expired and refreshes it if necessary.
// It now operates directly on the provided ActionConfig pointer to maintain independent consumer state.
func (s *SpotifyService) GetValidAuth(id string, cfgProvider ConfigProvider, cfg *models.ActionConfig) (models.AuthData, error) {
	auth := getAuth(cfg.AuthContext)
	if auth.AccessToken == "" {
		return models.AuthData{}, fmt.Errorf("no auth data found")
	}

	// Check if token is expired or about to expire in 5 minutes
	if !auth.Expiry.IsZero() && time.Now().Add(5*time.Minute).After(auth.Expiry) {
		log.Printf("[SpotifyService] Token expired or expiring soon for action %s, refreshing...", id)
		newAuth, err := s.RefreshAccessToken(auth)
		if err != nil {
			return models.AuthData{}, fmt.Errorf("refresh failed: %w", err)
		}

		// Update the cache
		for k := range cfg.AuthContext {
			cfg.AuthContext[k] = newAuth
			break // Spotify usually has one auth entry
		}
		if err := cfgProvider.UpdateAuth(id, cfg.AuthContext); err != nil {
			log.Printf("[SpotifyService] Warning: failed to update auth cache: %v", err)
		}
		return newAuth, nil
	}

	return auth, nil
}

func (s *SpotifyService) RefreshAccessToken(auth models.AuthData) (models.AuthData, error) {
	tokenURL := "https://accounts.spotify.com/api/token"
	clientID := os.Getenv("SPOTIFY_CLIENT_ID")
	clientSecret := os.Getenv("SPOTIFY_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		return models.AuthData{}, fmt.Errorf("SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not set")
	}

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", auth.RefreshToken)
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)

	req, err := http.NewRequest("POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return models.AuthData{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return models.AuthData{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.AuthData{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return models.AuthData{}, fmt.Errorf("refresh failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
		TokenType    string `json:"token_type"`
		Scope        string `json:"scope"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return models.AuthData{}, err
	}

	// Update auth fields
	auth.AccessToken = tokenResp.AccessToken
	if tokenResp.RefreshToken != "" {
		auth.RefreshToken = tokenResp.RefreshToken
	}
	auth.Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)

	log.Printf("[SpotifyService] Token refreshed successfully. Scopes: [%s], New expiry: %v", tokenResp.Scope, auth.Expiry)
	return auth, nil
}

func (s *SpotifyService) doRequest(method, endpoint string, auth models.AuthData, body []byte) (map[string]interface{}, int64, error) {
	start := time.Now()

	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequest(method, endpoint, bytes.NewBuffer(body))
	} else {
		req, err = http.NewRequest(method, endpoint, nil)
	}
	if err != nil {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("creating http request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	req.Header.Set("Content-Type", "application/json")

	log.Printf("[SpotifyService] Reqs: %s %s", method, endpoint)
	resp, err := s.httpClient.Do(req)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		return nil, elapsed, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, elapsed, fmt.Errorf("reading response body: %w", err)
	}
	log.Printf("[SpotifyService] Resp: %d", resp.StatusCode)

	var result map[string]interface{}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			// fallback if it's not JSON
			result = map[string]interface{}{"raw": string(respBody)}
		}
	} else {
		result = map[string]interface{}{"status": "success"}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errMsg := "API error"
		if errObj, ok := result["error"].(map[string]interface{}); ok {
			if msg, ok := errObj["message"].(string); ok {
				errMsg = msg
			}
		} else if strings.TrimSpace(string(respBody)) != "" {
			errMsg = string(respBody)
		}

		if resp.StatusCode == 403 {
			log.Printf("[SpotifyService] 403 Forbidden: This usually means missing scopes (playlist-modify-public/private) or you don't own the playlist/resource.")
		}

		return nil, elapsed, fmt.Errorf("spotify api returned %d: %s", resp.StatusCode, errMsg)
	}

	return result, elapsed, nil
}

func (s *SpotifyService) searchTrack(auth models.AuthData, query string) (string, error) {
	endpoint := fmt.Sprintf("%s/search?q=%s&type=track&limit=1", spotifyAPIBase, url.QueryEscape(query))
	res, _, err := s.doRequest("GET", endpoint, auth, nil)
	if err != nil {
		return "", err
	}

	tracks, ok := res["tracks"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("no tracks found response")
	}
	items, ok := tracks["items"].([]interface{})
	if !ok || len(items) == 0 {
		return "", fmt.Errorf("no tracks found for query: %s", query)
	}
	item, ok := items[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid track item")
	}
	id, ok := item["id"].(string)
	if !ok {
		return "", fmt.Errorf("invalid track id")
	}
	return id, nil
}

func (s *SpotifyService) getTrackURI(auth models.AuthData, trackID, trackQuery string) (string, error) {
	if trackID != "" {
		if strings.HasPrefix(trackID, "spotify:track:") {
			return trackID, nil
		}
		return "spotify:track:" + trackID, nil
	}
	if trackQuery != "" {
		id, err := s.searchTrack(auth, trackQuery)
		if err != nil {
			return "", err
		}
		return "spotify:track:" + id, nil
	}
	return "", fmt.Errorf("either track_id or track_query is required")
}

func (s *SpotifyService) getTrackID(auth models.AuthData, trackID, trackQuery string) (string, error) {
	if trackID != "" {
		if strings.HasPrefix(trackID, "spotify:track:") {
			return strings.TrimPrefix(trackID, "spotify:track:"), nil
		}
		return trackID, nil
	}
	if trackQuery != "" {
		return s.searchTrack(auth, trackQuery)
	}
	return "", fmt.Errorf("either track_id or track_query is required")
}

// 1. Add track to playback queue
func (s *SpotifyService) AddToQueue(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	uri, err := s.getTrackURI(auth, cfg.TrackID, cfg.TrackQuery)
	if err != nil {
		return nil, 0, err
	}
	endpoint := fmt.Sprintf("%s/me/player/queue?uri=%s", spotifyAPIBase, url.QueryEscape(uri))
	return s.doRequest("POST", endpoint, auth, nil)
}

// 2. Add track to a playlist by TrackID
func (s *SpotifyService) AddToPlaylistByID(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.PlaylistID == "" {
		return nil, 0, fmt.Errorf("playlist_id is required")
	}
	if cfg.TrackID == "" {
		return nil, 0, fmt.Errorf("track_id is required")
	}
	uri, err := s.getTrackURI(auth, cfg.TrackID, "")
	if err != nil {
		return nil, 0, err
	}
	endpoint := fmt.Sprintf("%s/playlists/%s/items", spotifyAPIBase, url.PathEscape(cfg.PlaylistID))
	body, _ := json.Marshal(map[string]interface{}{"uris": []string{uri}})
	return s.doRequest("POST", endpoint, auth, body)
}

// 3. Save a track
func (s *SpotifyService) SaveTrack(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	id, err := s.getTrackID(auth, cfg.TrackID, cfg.TrackQuery)
	if err != nil {
		return nil, 0, err
	}
	endpoint := fmt.Sprintf("%s/me/library?type=track&ids=%s", spotifyAPIBase, url.QueryEscape(id))
	return s.doRequest("PUT", endpoint, auth, nil)
}

// 4. Follow a playlist
func (s *SpotifyService) FollowPlaylist(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.PlaylistID == "" {
		return nil, 0, fmt.Errorf("playlist_id is required")
	}
	endpoint := fmt.Sprintf("%s/me/library?type=playlist&ids=%s", spotifyAPIBase, url.PathEscape(cfg.PlaylistID))
	return s.doRequest("PUT", endpoint, auth, nil)
}

// 5. Add track to a playlist (by search query)
func (s *SpotifyService) AddToPlaylist(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	if cfg.PlaylistID == "" {
		return nil, 0, fmt.Errorf("playlist_id is required")
	}
	if cfg.TrackQuery == "" && cfg.TrackID == "" {
		return nil, 0, fmt.Errorf("track_query or track_id is required")
	}
	uri, err := s.getTrackURI(auth, cfg.TrackID, cfg.TrackQuery)
	if err != nil {
		return nil, 0, err
	}
	endpoint := fmt.Sprintf("%s/playlists/%s/items", spotifyAPIBase, url.PathEscape(cfg.PlaylistID))
	body, _ := json.Marshal(map[string]interface{}{"uris": []string{uri}})
	return s.doRequest("POST", endpoint, auth, body)
}
