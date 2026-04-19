package services

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"youtube_trigger/models"
	"youtube_trigger/worker"

	"github.com/google/uuid"
)

// ConfigUpdater defines an interface to persist updated configuration context.
type ConfigUpdater interface {
	UpdateAuth(id string, auth map[string]models.AuthData) error
}

const youtubeAPIBase = "https://www.googleapis.com/youtube/v3"

// ── Safe type helpers ────────────────────────────────────────────────────────
// These prevent panics from nil interface{} assertions in deeply-nested API JSON.

func safeString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func safeMap(v interface{}) map[string]interface{} {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return nil
}

func safeSlice(v interface{}) []interface{} {
	if v == nil {
		return nil
	}
	if s, ok := v.([]interface{}); ok {
		return s
	}
	return nil
}

// ── Poller ────────────────────────────────────────────────────────────────────

type Poller struct {
	TriggerID      string
	WorkflowID     string
	CapabilityKey  string
	TriggerConfig  models.TriggerConfig
	Token          string
	RefreshToken   string
	Expiry         time.Time
	Provider       string
	APIKey         string
	SequenceNumber uint64
	Publisher      *worker.Publisher
	httpClient     *http.Client
	stopChan       chan struct{}
	mu             sync.Mutex
	Updater        ConfigUpdater

	// Ordered cache for change detection.
	// seenCache is a slice of IDs (oldest → newest); seenSet is the O(1) lookup mirror.
	seenCache   []string
	seenSet     map[string]bool
	isFirstPoll bool

	// Cached uploads playlist ID (fetched once, reused by pollMyUploads).
	uploadsPlaylistID string
}

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

// RefreshOAuth2Token handles refreshing tokens for various OAuth2 providers.
func RefreshOAuth2Token(provider, refreshToken string) (*TokenResponse, error) {
	var tokenURL string
	var clientID string
	var clientSecret string

	switch strings.ToLower(provider) {
	case "google", "youtube":
		tokenURL = "https://oauth2.googleapis.com/token"
		clientID = os.Getenv("GOOGLE_CLIENT_ID")
		clientSecret = os.Getenv("GOOGLE_CLIENT_SECRET")

	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}

	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("client credentials not found for provider %s", provider)
	}

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", refreshToken)
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)

	req, err := http.NewRequest("POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh failed: %s", string(body))
	}

	var tokenResp TokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, err
	}

	// Update refresh token if a new one is returned
	if tokenResp.RefreshToken == "" {
		tokenResp.RefreshToken = refreshToken
	}

	return &tokenResp, nil
}

func NewPoller(triggerID, workflowID string, config models.TriggerConfig, rawConfig map[string]interface{}, seq uint64, pub *worker.Publisher, updater ConfigUpdater) *Poller {
	// Extract auth
	var auth models.AuthData
	targets := []string{"google", "YouTube", "youtube", "google-oauth2", "google-oauth"}
	for _, target := range targets {
		if a, ok := config.AuthContext[target]; ok && a.AccessToken != "" {
			auth = a
			auth.Provider = target
			break
		}
	}

	// Extract API Key from auth or raw config
	apiKey := auth.APIKey
	if apiKey == "" {
		if val, ok := rawConfig["api_key"].(string); ok && val != "" {
			apiKey = val
		} else if val, ok := rawConfig["apiKey"].(string); ok && val != "" {
			apiKey = val
		}
	}

	return &Poller{
		TriggerID:      triggerID,
		WorkflowID:     workflowID,
		CapabilityKey:  config.CapabilityKey,
		TriggerConfig:  config,
		Token:          auth.AccessToken,
		RefreshToken:   auth.RefreshToken,
		Expiry:         auth.Expiry,
		Provider:       auth.Provider,
		APIKey:         apiKey,
		SequenceNumber: seq,
		Publisher:      pub,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		stopChan:       make(chan struct{}),
		seenCache:      make([]string, 0, 100),
		seenSet:        make(map[string]bool),
		isFirstPoll:    true,
		Updater:        updater,
	}
}

func (p *Poller) Start() {
	log.Printf("[Poller #%d] [Workflow: %s] Starting trigger=%s capability=%s", p.SequenceNumber, p.WorkflowID, p.TriggerID, p.CapabilityKey)

	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		p.poll()

		for {
			select {
			case <-ticker.C:
				p.poll()
			case <-p.stopChan:
				log.Printf("[Poller #%d] [Workflow: %s] Stopped trigger=%s", p.SequenceNumber, p.WorkflowID, p.TriggerID)
				return
			}
		}
	}()
}

func (p *Poller) Stop() {
	close(p.stopChan)
}

// doRequest is the public entry point; it allows one automatic retry on 401.
func (p *Poller) doRequest(method, endpoint string) (map[string]interface{}, error) {
	return p.executeRequest(method, endpoint, true)
}

// executeRequest performs the HTTP call. canRetry guards against infinite recursion
// when a refreshed token still returns 401.
func (p *Poller) executeRequest(method, endpoint string, canRetry bool) (map[string]interface{}, error) {
	// Append API key if present and not already in URL
	if p.APIKey != "" && !strings.Contains(endpoint, "key=") {
		separator := "?"
		if strings.Contains(endpoint, "?") {
			separator = "&"
		}
		endpoint = fmt.Sprintf("%s%skey=%s", endpoint, separator, p.APIKey)
	}

	req, err := http.NewRequest(method, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	tokenDisplay := "NONE"
	if p.Token != "" {
		if len(p.Token) > 8 {
			tokenDisplay = p.Token[:4] + "..." + p.Token[len(p.Token)-4:]
		} else {
			tokenDisplay = "***"
		}
	}
	log.Printf("[Poller #%d] [Workflow: %s] Request: %s %s (Token: %s)", p.SequenceNumber, p.WorkflowID, method, endpoint, tokenDisplay)

	token := strings.TrimSpace(p.Token)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized {
		if canRetry && p.RefreshToken != "" {
			log.Printf("[Poller #%d] [Workflow: %s] 401 Unauthorized for trigger=%s. Attempting token refresh...", p.SequenceNumber, p.WorkflowID, p.TriggerID)
			newTokens, err := RefreshOAuth2Token(p.Provider, p.RefreshToken)
			if err != nil {
				return nil, fmt.Errorf("token refresh failed: %w", err)
			}

			p.mu.Lock()
			p.Token = newTokens.AccessToken
			p.RefreshToken = newTokens.RefreshToken
			p.Expiry = time.Now().Add(time.Duration(newTokens.ExpiresIn) * time.Second)

			// Update the stored configuration context if an updater is provided
			if p.Updater != nil && p.Provider != "" {
				if p.TriggerConfig.AuthContext == nil {
					p.TriggerConfig.AuthContext = make(map[string]models.AuthData)
				}
				p.TriggerConfig.AuthContext[p.Provider] = models.AuthData{
					AccessToken:  p.Token,
					RefreshToken: p.RefreshToken,
					Expiry:       p.Expiry,
					Provider:     p.Provider,
				}
				_ = p.Updater.UpdateAuth(p.TriggerID, p.TriggerConfig.AuthContext)
			}
			p.mu.Unlock()

			log.Printf("[Poller #%d] [Workflow: %s] Token refreshed successfully for trigger=%s. Retrying request...", p.SequenceNumber, p.WorkflowID, p.TriggerID)
			// Retry once — canRetry=false prevents infinite recursion
			return p.executeRequest(method, endpoint, false)
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("api returned %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshaling response: %w", err)
	}

	return result, nil
}

// ── Cache management ─────────────────────────────────────────────────────────

// evictionCount returns how many of the oldest cache entries to drop when the
// cache exceeds 100 items. Slower-moving feeds use a smaller eviction count.
func (p *Poller) evictionCount() int {
	switch p.CapabilityKey {
	case "youtube_new_video_from_search",
		"youtube_new_liked_video",
		"youtube_subscribe_to_channel":
		return 2
	default:
		return 10
	}
}

// evictCache trims the oldest entries once the cache grows past 100.
func (p *Poller) evictCache() {
	const maxCacheSize = 100
	if len(p.seenCache) <= maxCacheSize {
		return
	}
	n := p.evictionCount()
	if n > len(p.seenCache) {
		n = len(p.seenCache)
	}
	for i := 0; i < n; i++ {
		delete(p.seenSet, p.seenCache[i])
	}
	p.seenCache = p.seenCache[n:]
}

// ── Poll loop ────────────────────────────────────────────────────────────────

func (p *Poller) poll() {
	log.Printf("[Poller #%d] [Workflow: %s] Polling YouTube for trigger=%s capability=%s",
		p.SequenceNumber, p.WorkflowID, p.TriggerID, p.CapabilityKey)

	var items []map[string]interface{}
	var err error

	switch p.CapabilityKey {
	case "youtube_new_video_from_search":
		items, err = p.pollSearch()
	case "youtube_new_liked_video":
		items, err = p.pollLikedVideos()
	case "youtube_subscribe_to_channel":
		items, err = p.pollSubscriptions()
	case "youtube_new_video_by_channel":
		items, err = p.pollChannelVideos()
	case "youtube_new_playlist":
		items, err = p.pollPlaylists()
	case "youtube_new_public_video_uploaded_by_you":
		items, err = p.pollMyUploads()
	case "youtube_new_public_video_from_subscriptions":
		items, err = p.pollSubscriptionActivities()
	case "youtube_new_super_chat_message":
		items, err = p.pollSuperChat()
	case "youtube_new_channel_membership":
		items, err = p.pollMemberships()
	case "youtube_new_super_sticker":
		items, err = p.pollSuperStickers()
	default:
		log.Printf("[Poller] Unknown capability: %s", p.CapabilityKey)
		return
	}

	if err != nil {
		log.Printf("[Poller #%d] [Workflow: %s] Error polling %s: %v", p.SequenceNumber, p.WorkflowID, p.CapabilityKey, err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isFirstPoll {
		// Seed the cache with every returned ID.
		for _, item := range items {
			id := safeString(item["_id"])
			if id == "" {
				continue
			}
			if !p.seenSet[id] {
				p.seenCache = append(p.seenCache, id)
				p.seenSet[id] = true
			}
		}
		// Fire an event for the latest item (index 0) if data exists.
		// This acts as an immediate test for the workflow.
		// Capabilities whose data does not exist (super chat, memberships, etc.)
		// will have an empty items list — no event is fired.
		if len(items) > 0 {
			first := items[0]
			if safeString(first["_id"]) != "" {
				p.fireEvent(first)
				log.Printf("[Poller #%d] [Workflow: %s] First-poll: fired event for latest item id=%s",
					p.SequenceNumber, p.WorkflowID, safeString(first["_id"]))
			}
		}
		log.Printf("[Poller #%d] [Workflow: %s] Seeded %d initial items for trigger=%s",
			p.SequenceNumber, p.WorkflowID, len(p.seenCache), p.TriggerID)
		p.isFirstPoll = false
		return
	}

	// Subsequent polls — fire events only for items NOT already in the cache.
	newCount := 0
	for _, item := range items {
		id := safeString(item["_id"])
		if id == "" {
			continue
		}
		if !p.seenSet[id] {
			p.fireEvent(item)
			p.seenCache = append(p.seenCache, id)
			p.seenSet[id] = true
			newCount++
		}
	}

	// Trim cache if it grew beyond the limit.
	p.evictCache()

	if newCount > 0 {
		log.Printf("[Poller #%d] [Workflow: %s] Detected %d new items for trigger=%s",
			p.SequenceNumber, p.WorkflowID, newCount, p.TriggerID)
	}
}

func (p *Poller) fireEvent(payload map[string]interface{}) {
	event := models.TriggerEvent{
		ID:            uuid.New().String(),
		WorkflowID:    p.WorkflowID,
		TriggerID:     p.TriggerID,
		Type:          "event",
		Name:          "YouTube Trigger",
		CapabilityKey: p.CapabilityKey,
		Payload:       payload,
		Timestamp:     time.Now().UTC(),
	}

	if err := p.Publisher.Publish(p.WorkflowID, event); err != nil {
		log.Printf("[Poller #%d] [Workflow: %s] Failed to publish event trigger=%s: %v", p.SequenceNumber, p.WorkflowID, p.TriggerID, err)
	} else {
		log.Printf("[Poller #%d] [Workflow: %s] Fired event capability=%s trigger=%s", p.SequenceNumber, p.WorkflowID, p.CapabilityKey, p.TriggerID)
	}
}

// ── Channel resolution helper ────────────────────────────────────────────────

// resolveChannelID converts a channel name, @handle, or custom URL into a
// canonical UC-prefixed channel ID. If the input already looks like an ID it is
// returned as-is. The resolved ID is cached on the Poller so this runs at most
// once per poller lifetime.
//
// Quota cost: 1 unit for handle lookup (channels.list), 100 units for
// fallback search (search.list). Only runs once — result is cached.
func (p *Poller) resolveChannelID(input string) (string, error) {
	// Already a channel ID
	if strings.HasPrefix(input, "UC") {
		return input, nil
	}

	// @handle → try the lightweight channels.list?forHandle endpoint (1 unit)
	if strings.HasPrefix(input, "@") {
		handle := strings.TrimPrefix(input, "@")
		endpoint := fmt.Sprintf("%s/channels?part=id&forHandle=%s",
			youtubeAPIBase, url.QueryEscape(handle))
		resp, err := p.doRequest("GET", endpoint)
		if err == nil {
			items := safeSlice(resp["items"])
			if len(items) > 0 {
				ch := safeMap(items[0])
				if id := safeString(ch["id"]); id != "" {
					return id, nil
				}
			}
		}
		// fall through to search
	}

	// Fallback: search for the channel by name (100 quota units, runs once)
	endpoint := fmt.Sprintf("%s/search?part=snippet&q=%s&type=channel&maxResults=1",
		youtubeAPIBase, url.QueryEscape(input))
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return "", fmt.Errorf("searching for channel: %w", err)
	}
	items := safeSlice(resp["items"])
	if len(items) == 0 {
		return "", fmt.Errorf("no channel found for: %s", input)
	}
	first := safeMap(items[0])
	idObj := safeMap(first["id"])
	channelID := safeString(idObj["channelId"])
	if channelID == "" {
		return "", fmt.Errorf("could not resolve channel ID for: %s", input)
	}
	return channelID, nil
}

// ── Capability poll functions ────────────────────────────────────────────────

// pollSearch calls YouTube search.list for videos matching the configured query.
// Quota cost: 100 units per call (search.list is expensive).
func (p *Poller) pollSearch() ([]map[string]interface{}, error) {
	query := p.TriggerConfig.SearchQuery
	if query == "" {
		return nil, fmt.Errorf("search query is required for youtube_new_video_from_search")
	}

	endpoint := fmt.Sprintf("%s/search?part=snippet&q=%s&type=video&order=date&maxResults=25",
		youtubeAPIBase, url.QueryEscape(query))

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		snippet := safeMap(v["snippet"])
		idObj := safeMap(v["id"])
		videoID := safeString(idObj["videoId"])
		if videoID == "" {
			continue
		}

		results = append(results, map[string]interface{}{
			"_id":          videoID,
			"title":        safeString(snippet["title"]),
			"description":  safeString(snippet["description"]),
			"url":          "https://www.youtube.com/watch?v=" + videoID,
			"author_name":  safeString(snippet["channelTitle"]),
			"published_at": safeString(snippet["publishedAt"]),
		})
	}
	return results, nil
}

// pollLikedVideos calls YouTube videos.list with myRating=like.
// Requires OAuth 2.0. Quota cost: 1 unit per call.
func (p *Poller) pollLikedVideos() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/videos?part=snippet&myRating=like&maxResults=50", youtubeAPIBase)

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		// videos.list returns id as a plain string (not an object)
		videoID := safeString(v["id"])
		if videoID == "" {
			continue
		}

		snippet := safeMap(v["snippet"])
		results = append(results, map[string]interface{}{
			"_id":         videoID,
			"title":       safeString(snippet["title"]),
			"description": safeString(snippet["description"]),
			"url":         "https://www.youtube.com/watch?v=" + videoID,
			"author_name": safeString(snippet["channelTitle"]),
			// The YouTube API does not expose when a video was liked;
			// we use the current timestamp as a best-effort approximation.
			"liked_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
	return results, nil
}

// pollSubscriptions calls YouTube subscriptions.list for the authenticated user.
// Requires OAuth 2.0. Quota cost: 1 unit per call.
func (p *Poller) pollSubscriptions() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/subscriptions?part=snippet&mine=true&maxResults=50",
		youtubeAPIBase)

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	// Collect raw entries with their timestamps for sorting.
	type subEntry struct {
		channelID   string
		channelName string
		description string
		publishedAt string
	}

	var entries []subEntry
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		snippet := safeMap(v["snippet"])
		resID := safeMap(snippet["resourceId"])
		channelID := safeString(resID["channelId"])
		if channelID == "" {
			continue
		}
		entries = append(entries, subEntry{
			channelID:   channelID,
			channelName: safeString(snippet["title"]),
			description: safeString(snippet["description"]),
			publishedAt: safeString(snippet["publishedAt"]),
		})
	}

	// Sort newest subscription first so items[0] is the most recent.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].publishedAt > entries[j].publishedAt // RFC3339 sorts lexicographically
	})

	var results []map[string]interface{}
	for _, e := range entries {
		results = append(results, map[string]interface{}{
			"_id":          e.channelID,
			"channel_name": e.channelName,
			"channel_url":  "https://www.youtube.com/channel/" + e.channelID,
			"description":  e.description,
		})
	}
	return results, nil
}

// pollChannelVideos calls YouTube search.list scoped to a specific channel.
// If the configured value is a channel name or @handle instead of a UC-prefixed
// ID, it is resolved once and cached.
// Quota cost: 100 units per call (search.list). Resolution adds 1–100 units on
// first poll only.
func (p *Poller) pollChannelVideos() ([]map[string]interface{}, error) {
	channelID := p.TriggerConfig.ChannelNameOrID
	if channelID == "" {
		return nil, fmt.Errorf("channel name or ID is required for youtube_new_video_by_channel")
	}

	// Resolve channel name/handle → ID once; result is cached on the Poller.
	if !strings.HasPrefix(channelID, "UC") {
		resolved, err := p.resolveChannelID(channelID)
		if err != nil {
			return nil, fmt.Errorf("resolving channel: %w", err)
		}
		p.TriggerConfig.ChannelNameOrID = resolved
		channelID = resolved
		log.Printf("[Poller #%d] [Workflow: %s] Resolved channel to ID: %s",
			p.SequenceNumber, p.WorkflowID, channelID)
	}

	endpoint := fmt.Sprintf("%s/search?part=snippet&channelId=%s&type=video&order=date&maxResults=25",
		youtubeAPIBase, url.QueryEscape(channelID))

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		snippet := safeMap(v["snippet"])
		idObj := safeMap(v["id"])
		videoID := safeString(idObj["videoId"])
		if videoID == "" {
			continue
		}

		results = append(results, map[string]interface{}{
			"_id":          videoID,
			"title":        safeString(snippet["title"]),
			"description":  safeString(snippet["description"]),
			"url":          "https://www.youtube.com/watch?v=" + videoID,
			"author_name":  safeString(snippet["channelTitle"]),
			"published_at": safeString(snippet["publishedAt"]),
		})
	}
	return results, nil
}

// pollPlaylists calls YouTube playlists.list for the authenticated user.
// Requires OAuth 2.0. Quota cost: 1 unit per call.
func (p *Poller) pollPlaylists() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/playlists?part=snippet&mine=true&maxResults=50", youtubeAPIBase)

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	// Collect and sort so the newest playlist is first.
	type plEntry struct {
		id          string
		title       string
		description string
		publishedAt string
	}

	var entries []plEntry
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		playlistID := safeString(v["id"])
		if playlistID == "" {
			continue
		}
		snippet := safeMap(v["snippet"])
		entries = append(entries, plEntry{
			id:          playlistID,
			title:       safeString(snippet["title"]),
			description: safeString(snippet["description"]),
			publishedAt: safeString(snippet["publishedAt"]),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].publishedAt > entries[j].publishedAt
	})

	var results []map[string]interface{}
	for _, e := range entries {
		results = append(results, map[string]interface{}{
			"_id":          e.id,
			"title":        e.title,
			"description":  e.description,
			"url":          "https://www.youtube.com/playlist?list=" + e.id,
			"published_at": e.publishedAt,
		})
	}
	return results, nil
}

// pollMyUploads retrieves the authenticated user's uploaded videos using the
// officially recommended two-step approach:
//   1. channels.list?mine=true → get the uploads playlist ID (cached after first call)
//   2. playlistItems.list?playlistId=... → list videos from that playlist
//
// Quota cost: 2 units on first poll (1 channels + 1 playlistItems), 1 unit on
// subsequent polls (playlistItems only, since the playlist ID is cached).
func (p *Poller) pollMyUploads() ([]map[string]interface{}, error) {
	// Step 1: Fetch and cache the uploads playlist ID.
	if p.uploadsPlaylistID == "" {
		endpoint := fmt.Sprintf("%s/channels?part=contentDetails&mine=true", youtubeAPIBase)
		resp, err := p.doRequest("GET", endpoint)
		if err != nil {
			return nil, fmt.Errorf("fetching channel info: %w", err)
		}
		items := safeSlice(resp["items"])
		if len(items) == 0 {
			return nil, fmt.Errorf("no channel found for authenticated user")
		}
		ch := safeMap(items[0])
		contentDetails := safeMap(ch["contentDetails"])
		relatedPlaylists := safeMap(contentDetails["relatedPlaylists"])
		p.uploadsPlaylistID = safeString(relatedPlaylists["uploads"])
		if p.uploadsPlaylistID == "" {
			return nil, fmt.Errorf("could not find uploads playlist")
		}
		log.Printf("[Poller #%d] [Workflow: %s] Cached uploads playlist ID: %s",
			p.SequenceNumber, p.WorkflowID, p.uploadsPlaylistID)
	}

	// Step 2: List items from the uploads playlist.
	endpoint := fmt.Sprintf("%s/playlistItems?part=snippet,contentDetails&playlistId=%s&maxResults=25",
		youtubeAPIBase, url.QueryEscape(p.uploadsPlaylistID))

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		snippet := safeMap(v["snippet"])
		contentDetails := safeMap(v["contentDetails"])

		// Prefer contentDetails.videoId; fall back to snippet.resourceId.videoId
		videoID := safeString(contentDetails["videoId"])
		if videoID == "" {
			resID := safeMap(snippet["resourceId"])
			videoID = safeString(resID["videoId"])
		}
		if videoID == "" {
			continue
		}

		// Use the actual video publish date when available.
		publishedAt := safeString(contentDetails["videoPublishedAt"])
		if publishedAt == "" {
			publishedAt = safeString(snippet["publishedAt"])
		}

		results = append(results, map[string]interface{}{
			"_id":          videoID,
			"title":        safeString(snippet["title"]),
			"description":  safeString(snippet["description"]),
			"url":          "https://www.youtube.com/watch?v=" + videoID,
			"embed_code":   fmt.Sprintf(`<iframe width="560" height="315" src="https://www.youtube.com/embed/%s" frameborder="0" allowfullscreen></iframe>`, videoID),
			"published_at": publishedAt,
		})
	}
	return results, nil
}

// pollSubscriptionActivities retrieves recent videos published by channels the
// authenticated user is subscribed to.
//
// NOTE: The activities.list?home=true parameter has been deprecated by Google.
// This implementation fetches the user's subscriptions, then queries each
// subscribed channel's recent upload activities individually.
//
// Quota cost: 1 unit (subscriptions.list) + N units (activities.list per
// channel, default N=10). Total ≈ 11 units per poll.
func (p *Poller) pollSubscriptionActivities() ([]map[string]interface{}, error) {
	// Step 1: Get the user's subscriptions (limited to 10 to save quota).
	subEndpoint := fmt.Sprintf("%s/subscriptions?part=snippet&mine=true&maxResults=10",
		youtubeAPIBase)
	subResp, err := p.doRequest("GET", subEndpoint)
	if err != nil {
		return nil, fmt.Errorf("fetching subscriptions: %w", err)
	}
	subItems := safeSlice(subResp["items"])
	if len(subItems) == 0 {
		return nil, nil // no subscriptions
	}

	var allResults []map[string]interface{}

	// Step 2: For each subscribed channel, fetch recent upload activities.
	for _, sub := range subItems {
		subMap := safeMap(sub)
		snippet := safeMap(subMap["snippet"])
		resID := safeMap(snippet["resourceId"])
		channelID := safeString(resID["channelId"])
		if channelID == "" {
			continue
		}

		actEndpoint := fmt.Sprintf("%s/activities?part=snippet,contentDetails&channelId=%s&maxResults=5",
			youtubeAPIBase, url.QueryEscape(channelID))
		actResp, err := p.doRequest("GET", actEndpoint)
		if err != nil {
			log.Printf("[Poller #%d] [Workflow: %s] Error fetching activities for channel %s: %v",
				p.SequenceNumber, p.WorkflowID, channelID, err)
			continue
		}

		for _, act := range safeSlice(actResp["items"]) {
			actMap := safeMap(act)
			actSnippet := safeMap(actMap["snippet"])
			// Only include upload activities, skip likes/comments/etc.
			if safeString(actSnippet["type"]) != "upload" {
				continue
			}

			details := safeMap(actMap["contentDetails"])
			upload := safeMap(details["upload"])
			videoID := safeString(upload["videoId"])
			if videoID == "" {
				continue
			}

			allResults = append(allResults, map[string]interface{}{
				"_id":          videoID,
				"title":        safeString(actSnippet["title"]),
				"description":  safeString(actSnippet["description"]),
				"url":          "https://www.youtube.com/watch?v=" + videoID,
				"author_name":  safeString(actSnippet["channelTitle"]),
				"published_at": safeString(actSnippet["publishedAt"]),
			})
		}
	}

	// Sort newest first so items[0] is the most recently published video.
	sort.Slice(allResults, func(i, j int) bool {
		return safeString(allResults[i]["published_at"]) > safeString(allResults[j]["published_at"])
	})

	return allResults, nil
}

// pollSuperChat calls YouTube superChatEvents.list for the authenticated user.
// Requires OAuth 2.0 and the channel must have Super Chat enabled (YouTube
// Partner Program — channel monetization required).
//
// NOTE: No separate payment is required beyond normal YouTube API quota (1 unit
// per call), but the channel MUST be enrolled in the YouTube Partner Program
// with Super Chat enabled. Channels without monetization will receive 403.
func (p *Poller) pollSuperChat() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/superChatEvents?part=snippet&maxResults=50", youtubeAPIBase)

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		// Gracefully handle 403 — channel is not monetized / Super Chat not enabled.
		if strings.Contains(err.Error(), "403") {
			log.Printf("[Poller #%d] [Workflow: %s] Super Chat not available (requires YouTube Partner Program): %v",
				p.SequenceNumber, p.WorkflowID, err)
			return nil, nil
		}
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		snippet := safeMap(v["snippet"])

		// Skip Super Sticker events — those are handled by pollSuperStickers.
		if isSuperSticker, ok := snippet["isSuperStickerEvent"].(bool); ok && isSuperSticker {
			continue
		}

		id := safeString(v["id"])
		if id == "" {
			continue
		}

		supporterDetails := safeMap(snippet["supporterDetails"])

		// amountMicros may be a string or a number depending on API version.
		amountStr := formatAmountMicros(snippet["amountMicros"], safeString(snippet["displayString"]))

		results = append(results, map[string]interface{}{
			"_id":          id,
			"message":      safeString(snippet["commentText"]),
			"author_name":  safeString(supporterDetails["displayName"]),
			"amount":       amountStr,
			"currency":     safeString(snippet["currency"]),
			"published_at": safeString(snippet["createdAt"]),
		})
	}
	return results, nil
}

// pollMemberships calls YouTube members.list for the authenticated user's channel.
// Requires OAuth 2.0 with the youtube.channel-memberships.creator scope.
//
// NOTE: This endpoint requires BOTH:
//   1. The channel must have channel memberships enabled.
//   2. The API project must be explicitly approved by Google for access to this
//      endpoint. Most projects will receive 403 Forbidden without this approval.
//      Contact your Google/YouTube representative to request access.
//
// Quota cost: 1 unit per call (no separate payment required).
func (p *Poller) pollMemberships() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/members?part=snippet&mode=listMembers&maxResults=50", youtubeAPIBase)

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		// Gracefully handle 403/401 — channel memberships not enabled or API not approved.
		if strings.Contains(err.Error(), "403") || strings.Contains(err.Error(), "401") {
			log.Printf("[Poller #%d] [Workflow: %s] Members API not available (requires memberships + API approval): %v",
				p.SequenceNumber, p.WorkflowID, err)
			return nil, nil
		}
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		snippet := safeMap(v["snippet"])
		memberDetails := safeMap(snippet["memberDetails"])
		membershipsDetails := safeMap(snippet["membershipsDetails"])
		membershipsDuration := safeMap(membershipsDetails["membershipsDuration"])

		// Use the member's channel ID as unique identifier.
		memberChannelID := safeString(memberDetails["channelId"])
		if memberChannelID == "" {
			continue
		}

		results = append(results, map[string]interface{}{
			"_id":         memberChannelID,
			"member_name": safeString(memberDetails["displayName"]),
			"level":       safeString(membershipsDetails["highestAccessibleLevelDisplayName"]),
			"joined_at":   safeString(membershipsDuration["memberSince"]),
		})
	}
	return results, nil
}

// pollSuperStickers calls YouTube superChatEvents.list and filters for sticker events.
// Requires OAuth 2.0 and the channel must have Super Chat / Super Stickers
// enabled (YouTube Partner Program — channel monetization required).
//
// NOTE: No separate payment is required beyond normal YouTube API quota (1 unit
// per call), but the channel MUST be enrolled in the YouTube Partner Program
// with Super Stickers enabled. Channels without monetization will receive 403.
func (p *Poller) pollSuperStickers() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/superChatEvents?part=snippet&maxResults=50", youtubeAPIBase)

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		// Gracefully handle 403 — channel is not monetized.
		if strings.Contains(err.Error(), "403") {
			log.Printf("[Poller #%d] [Workflow: %s] Super Stickers not available (requires YouTube Partner Program): %v",
				p.SequenceNumber, p.WorkflowID, err)
			return nil, nil
		}
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(resp["items"]) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		snippet := safeMap(v["snippet"])

		// Only include Super Sticker events.
		isSuperSticker, ok := snippet["isSuperStickerEvent"].(bool)
		if !ok || !isSuperSticker {
			continue
		}

		id := safeString(v["id"])
		if id == "" {
			continue
		}

		supporterDetails := safeMap(snippet["supporterDetails"])
		stickerMetadata := safeMap(snippet["superStickerMetadata"])

		// The API provides a stickerId; YouTube does not expose a direct image URL
		// for stickers via this endpoint, so the stickerId serves as the reference.
		stickerURL := safeString(stickerMetadata["stickerId"])

		amountStr := formatAmountMicros(snippet["amountMicros"], safeString(snippet["displayString"]))

		results = append(results, map[string]interface{}{
			"_id":          id,
			"sticker_url":  stickerURL,
			"author_name":  safeString(supporterDetails["displayName"]),
			"amount":       amountStr,
			"currency":     safeString(snippet["currency"]),
			"published_at": safeString(snippet["createdAt"]),
		})
	}
	return results, nil
}

// ── Shared helpers ───────────────────────────────────────────────────────────

// formatAmountMicros safely converts the amountMicros field (which may arrive as
// either a JSON string or a JSON number) into a human-readable currency string.
func formatAmountMicros(raw interface{}, fallback string) string {
	switch amt := raw.(type) {
	case string:
		var f float64
		if _, err := fmt.Sscanf(amt, "%f", &f); err == nil {
			return fmt.Sprintf("%.2f", f/1000000)
		}
		return amt
	case float64:
		return fmt.Sprintf("%.2f", amt/1000000)
	default:
		if fallback != "" {
			return fallback
		}
		return "0.00"
	}
}
