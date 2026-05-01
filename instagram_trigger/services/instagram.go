package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"instagram_trigger/models"
	"instagram_trigger/worker"

	"github.com/google/uuid"
)

// ── Instagram Graph API ──────────────────────────────────────────────────────
//
// The Instagram Basic Display API was permanently shut down by Meta on
// December 4, 2024. All integrations MUST use the Instagram Graph API,
// which requires a Professional (Business or Creator) Instagram account.
//
// Base URL: https://graph.instagram.com
// Endpoint: GET /me/media?fields=id,caption,media_type,media_url,permalink,timestamp&limit=50
//
// NOTE: No separate payment is required, but the Instagram account MUST be
// a Business or Creator account. Personal accounts will receive 400 errors.
// Rate limit: 200 calls per user per hour.

const instagramAPIBase = "https://graph.instagram.com"

// ── Safe type helpers ────────────────────────────────────────────────────────

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

// ── ConfigUpdater ────────────────────────────────────────────────────────────

// ConfigUpdater defines an interface to persist updated auth context.
type ConfigUpdater interface {
	UpdateAuth(id string, auth map[string]models.AuthData) error
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
	SequenceNumber uint64
	Publisher      *worker.Publisher
	httpClient     *http.Client
	stopChan       chan struct{}
	stopOnce       sync.Once
	mu             sync.Mutex
	Updater        ConfigUpdater

	// Ordered cache for change detection.
	seenCache   []string
	seenSet     map[string]bool
	isFirstPoll bool
}

func NewPoller(triggerID, workflowID string, config models.TriggerConfig, seq uint64, pub *worker.Publisher, updater ConfigUpdater) *Poller {
	// Extract auth — try known Instagram/Facebook provider keys
	var auth models.AuthData
	targets := []string{"instagram", "facebook", "instagram-oauth2", "facebook-oauth2", "meta"}
	for _, target := range targets {
		if a, ok := config.AuthContext[target]; ok && a.AccessToken != "" {
			auth = a
			auth.Provider = target
			break
		}
	}

	// Fallback: take the first one with an access token
	if auth.AccessToken == "" {
		for provider, a := range config.AuthContext {
			if a.AccessToken != "" {
				auth = a
				auth.Provider = provider
				break
			}
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
		SequenceNumber: seq,
		Publisher:      pub,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSHandshakeTimeout: 30 * time.Second,
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					dialer := &net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
					}
					return dialer.DialContext(ctx, "tcp4", addr)
				},
				MaxIdleConns:          10,
				IdleConnTimeout:       90 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
		stopChan:    make(chan struct{}),
		seenCache:   make([]string, 0, 100),
		seenSet:     make(map[string]bool),
		isFirstPoll: true,
		Updater:     updater,
	}
}

// ── Token refresh ────────────────────────────────────────────────────────────

// RefreshAccessToken refreshes an Instagram/Facebook long-lived token.
// Instagram long-lived tokens are valid for 60 days and can be refreshed
// before they expire using:
//
//	GET https://graph.instagram.com/refresh_access_token
//	    ?grant_type=ig_refresh_token&access_token={long-lived-token}
//
// If the token was obtained via Facebook Login, use the Facebook token exchange:
//
//	GET https://graph.facebook.com/v22.0/oauth/access_token
//	    ?grant_type=fb_exchange_token&client_id=...&client_secret=...&fb_exchange_token={token}
func (p *Poller) RefreshAccessToken() error {
	// Try Instagram-native refresh first (works for tokens obtained via Instagram Login)
	endpoint := fmt.Sprintf("%s/refresh_access_token?grant_type=ig_refresh_token&access_token=%s",
		instagramAPIBase, url.QueryEscape(p.Token))

	resp, err := p.httpClient.Get(endpoint)
	if err == nil && resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var tokenResp struct {
			AccessToken string `json:"access_token"`
			TokenType   string `json:"token_type"`
			ExpiresIn   int    `json:"expires_in"`
		}
		if err := json.Unmarshal(body, &tokenResp); err == nil && tokenResp.AccessToken != "" {
			p.Token = tokenResp.AccessToken
			p.Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
			log.Printf("[Poller #%d] [Workflow: %s] Instagram token refreshed (ig_refresh_token). New expiry: %v",
				p.SequenceNumber, p.WorkflowID, p.Expiry)
			p.persistRefreshedAuth()
			return nil
		}
	}
	if resp != nil {
		resp.Body.Close()
	}

	// Fallback: Facebook token exchange
	clientID := os.Getenv("FACEBOOK_CLIENT_ID")
	if clientID == "" {
		clientID = os.Getenv("INSTAGRAM_CLIENT_ID")
	}
	clientSecret := os.Getenv("FACEBOOK_CLIENT_SECRET")
	if clientSecret == "" {
		clientSecret = os.Getenv("INSTAGRAM_CLIENT_SECRET")
	}

	if clientID != "" && clientSecret != "" {
		fbEndpoint := fmt.Sprintf("https://graph.facebook.com/v22.0/oauth/access_token?grant_type=fb_exchange_token&client_id=%s&client_secret=%s&fb_exchange_token=%s",
			url.QueryEscape(clientID), url.QueryEscape(clientSecret), url.QueryEscape(p.Token))

		fbResp, err := p.httpClient.Get(fbEndpoint)
		if err == nil && fbResp.StatusCode == http.StatusOK {
			defer fbResp.Body.Close()
			body, _ := io.ReadAll(fbResp.Body)
			var tokenResp struct {
				AccessToken string `json:"access_token"`
				TokenType   string `json:"token_type"`
				ExpiresIn   int    `json:"expires_in"`
			}
			if err := json.Unmarshal(body, &tokenResp); err == nil && tokenResp.AccessToken != "" {
				p.Token = tokenResp.AccessToken
				p.Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
				log.Printf("[Poller #%d] [Workflow: %s] Instagram token refreshed (fb_exchange_token). New expiry: %v",
					p.SequenceNumber, p.WorkflowID, p.Expiry)
				p.persistRefreshedAuth()
				return nil
			}
		}
		if fbResp != nil {
			fbResp.Body.Close()
		}
	}

	return fmt.Errorf("failed to refresh Instagram access token")
}

// persistRefreshedAuth updates the stored config with the new token.
func (p *Poller) persistRefreshedAuth() {
	if p.Updater == nil || p.Provider == "" {
		return
	}
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

func (p *Poller) Start() {
	log.Printf("[Poller #%d] [Workflow: %s] Starting trigger=%s capability=%s",
		p.SequenceNumber, p.WorkflowID, p.TriggerID, p.CapabilityKey)

	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		p.poll()

		for {
			select {
			case <-ticker.C:
				p.poll()
			case <-p.stopChan:
				log.Printf("[Poller #%d] [Workflow: %s] Stopped trigger=%s",
					p.SequenceNumber, p.WorkflowID, p.TriggerID)
				return
			}
		}
	}()
}

func (p *Poller) Stop() {
	p.stopOnce.Do(func() { close(p.stopChan) })
}

// ── HTTP request helper ──────────────────────────────────────────────────────

// doRequest performs an HTTP GET against the Instagram Graph API with automatic
// retry on 401 (token expired). canRetry guards against infinite recursion.
func (p *Poller) doRequest(endpoint string) (map[string]interface{}, error) {
	return p.executeRequest(endpoint, true)
}

func (p *Poller) executeRequest(endpoint string, canRetry bool) (map[string]interface{}, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
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

	// Handle 401 — attempt token refresh and retry once
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusBadRequest {
		// Instagram Graph API returns 400 (not 401) for expired tokens with
		// error type "OAuthException". Check both status codes.
		if canRetry {
			log.Printf("[Poller #%d] [Workflow: %s] HTTP %d — attempting token refresh for trigger=%s",
				p.SequenceNumber, p.WorkflowID, resp.StatusCode, p.TriggerID)
			if refreshErr := p.RefreshAccessToken(); refreshErr == nil {
				// Rebuild the endpoint URL with the new token
				newEndpoint := replaceTokenInURL(endpoint, p.Token)
				return p.executeRequest(newEndpoint, false)
			} else {
				log.Printf("[Poller #%d] [Workflow: %s] Token refresh failed: %v",
					p.SequenceNumber, p.WorkflowID, refreshErr)
			}
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("instagram api returned %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshaling response: %w", err)
	}

	return result, nil
}

// replaceTokenInURL swaps the access_token query parameter in a URL.
func replaceTokenInURL(rawURL, newToken string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := parsed.Query()
	q.Set("access_token", newToken)
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

// ── Cache management ─────────────────────────────────────────────────────────

// evictCache trims the oldest 10 entries once the cache grows past 100.
func (p *Poller) evictCache() {
	const maxCacheSize = 100
	const evictCount = 10
	if len(p.seenCache) <= maxCacheSize {
		return
	}
	n := evictCount
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
	log.Printf("[Poller #%d] [Workflow: %s] Polling Instagram for trigger=%s capability=%s",
		p.SequenceNumber, p.WorkflowID, p.TriggerID, p.CapabilityKey)

	var items []map[string]interface{}
	var err error

	switch p.CapabilityKey {
	case "any_new_photo_by_you":
		items, err = p.pollMedia("IMAGE")
	case "new_photo_by_you_with_hashtag":
		items, err = p.pollMediaWithHashtag("IMAGE")
	case "any_new_video_by_you":
		items, err = p.pollMedia("VIDEO")
	case "new_video_by_you_with_hashtag":
		items, err = p.pollMediaWithHashtag("VIDEO")
	default:
		log.Printf("[Poller] Unknown capability: %s", p.CapabilityKey)
		return
	}

	if err != nil {
		log.Printf("[Poller #%d] [Workflow: %s] Error polling %s: %v",
			p.SequenceNumber, p.WorkflowID, p.CapabilityKey, err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isFirstPoll {
		// Seed cache with all returned IDs.
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
		// Fire event for the latest item (index 0) if data exists.
		// This acts as an immediate test for the workflow.
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

	// Subsequent polls — fire only for items NOT in cache.
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
		Name:          "Instagram Trigger",
		CapabilityKey: p.CapabilityKey,
		Payload:       payload,
		Timestamp:     time.Now().UTC(),
	}

	if err := p.Publisher.Publish(p.WorkflowID, event); err != nil {
		log.Printf("[Poller #%d] [Workflow: %s] Failed to publish event trigger=%s: %v",
			p.SequenceNumber, p.WorkflowID, p.TriggerID, err)
	} else {
		log.Printf("[Poller #%d] [Workflow: %s] Fired event capability=%s trigger=%s",
			p.SequenceNumber, p.WorkflowID, p.CapabilityKey, p.TriggerID)
	}
}

// ── Capability poll functions ────────────────────────────────────────────────

// pollMedia calls the Instagram Graph API to fetch the authenticated user's
// media and filters by the given media type ("IMAGE" or "VIDEO").
//
// Endpoint:
//
//	GET https://graph.instagram.com/me/media
//	    ?fields=id,caption,media_type,media_url,permalink,timestamp
//	    &limit=50
//	    &access_token={token}
//
// media_type values returned by the API:
//   - IMAGE  — photos
//   - VIDEO  — videos and reels
//   - CAROUSEL_ALBUM — multi-image/video posts (we check individual items
//     if we need to, but for simplicity we include carousels when their
//     top-level type matches)
//
// Rate limit: 200 calls per user per hour (free, no payment required).
// Requires a Business or Creator Instagram account.
func (p *Poller) pollMedia(mediaType string) ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/me/media?fields=id,caption,media_type,media_url,permalink,timestamp&limit=50&access_token=%s",
		instagramAPIBase, url.QueryEscape(p.Token))

	resp, err := p.doRequest(endpoint)
	if err != nil {
		return nil, err
	}

	return p.parseMediaResponse(resp, mediaType, "")
}

// pollMediaWithHashtag calls the same /me/media endpoint and filters results
// by both media type AND hashtag presence in the caption.
func (p *Poller) pollMediaWithHashtag(mediaType string) ([]map[string]interface{}, error) {
	hashtag := p.TriggerConfig.Hashtag
	if hashtag == "" {
		return nil, fmt.Errorf("hashtag is required for %s", p.CapabilityKey)
	}

	endpoint := fmt.Sprintf("%s/me/media?fields=id,caption,media_type,media_url,permalink,timestamp&limit=50&access_token=%s",
		instagramAPIBase, url.QueryEscape(p.Token))

	resp, err := p.doRequest(endpoint)
	if err != nil {
		return nil, err
	}

	return p.parseMediaResponse(resp, mediaType, hashtag)
}

// parseMediaResponse processes the Instagram Graph API response, filtering by
// media type and optionally by hashtag.
func (p *Poller) parseMediaResponse(resp map[string]interface{}, mediaType, hashtag string) ([]map[string]interface{}, error) {
	dataSlice := safeSlice(resp["data"])
	if len(dataSlice) == 0 {
		return nil, nil
	}

	// Normalise the hashtag for matching
	var normalizedTag string
	if hashtag != "" {
		normalizedTag = strings.ToLower(strings.TrimPrefix(hashtag, "#"))
	}

	var results []map[string]interface{}
	for _, item := range dataSlice {
		m := safeMap(item)
		if m == nil {
			continue
		}

		// Filter by media type.
		// IMAGE filter also matches CAROUSEL_ALBUM (which are primarily photo posts).
		itemType := safeString(m["media_type"])
		matchesType := false
		if mediaType == "IMAGE" {
			matchesType = itemType == "IMAGE" || itemType == "CAROUSEL_ALBUM"
		} else if mediaType == "VIDEO" {
			matchesType = itemType == "VIDEO" || itemType == "REELS"
		}
		if !matchesType {
			continue
		}

		// Filter by hashtag if specified.
		caption := safeString(m["caption"])
		if normalizedTag != "" {
			if !containsHashtag(caption, normalizedTag) {
				continue
			}
		}

		mediaID := safeString(m["id"])
		if mediaID == "" {
			continue
		}

		results = append(results, map[string]interface{}{
			"_id":        mediaID,
			"caption":    caption,
			"url":        safeString(m["permalink"]),
			"source_url": safeString(m["media_url"]),
			"created_at": safeString(m["timestamp"]),
		})
	}

	return results, nil
}

// containsHashtag checks if a caption contains the specified hashtag.
// It handles the hashtag with or without the # prefix, and performs a
// case-insensitive match.
func containsHashtag(caption, normalizedTag string) bool {
	lowerCaption := strings.ToLower(caption)
	// Look for #tag as a substring
	return strings.Contains(lowerCaption, "#"+normalizedTag)
}
