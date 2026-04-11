package services

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"youtube_trigger/models"
	"youtube_trigger/worker"

	"sync"

	"github.com/google/uuid"
)

const youtubeAPIBase = "https://www.googleapis.com/youtube/v3"

type Poller struct {
	TriggerID     string
	WorkflowID    string
	CapabilityKey string
	Config        map[string]interface{}
	Token         string
	RefreshToken  string
	Expiry        time.Time
	Provider      string
	APIKey        string
	Publisher     *worker.Publisher
	httpClient    *http.Client
	stopChan      chan struct{}
	lastCheck     time.Time
	mu            sync.Mutex
	seenIDs       map[string]bool
	isFirstPoll   bool
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

func NewPoller(triggerID, workflowID, capabilityKey string, config map[string]interface{}, auth models.AuthData, apiKey string, pub *worker.Publisher) *Poller {
	return &Poller{
		TriggerID:     triggerID,
		WorkflowID:    workflowID,
		CapabilityKey: capabilityKey,
		Config:        config,
		Token:         auth.AccessToken,
		RefreshToken:  auth.RefreshToken,
		Expiry:        auth.Expiry,
		Provider:      auth.Provider,
		APIKey:        apiKey,
		Publisher:     pub,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		stopChan:      make(chan struct{}),
		lastCheck:     time.Now().UTC(),
		seenIDs:       make(map[string]bool),
		isFirstPoll:   true,
	}
}

func (p *Poller) Start() {
	log.Printf("[Poller] Starting trigger=%s workflow=%s capability=%s", p.TriggerID, p.WorkflowID, p.CapabilityKey)

	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()

		p.poll()

		for {
			select {
			case <-ticker.C:
				p.poll()
			case <-p.stopChan:
				log.Printf("[Poller] Stopped trigger=%s", p.TriggerID)
				return
			}
		}
	}()
}

func (p *Poller) Stop() {
	close(p.stopChan)
}

func (p *Poller) doRequest(method, endpoint string) (map[string]interface{}, error) {
	// Add API key if present and not already in URL
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
	log.Printf("[Poller] Request: %s %s (Token: %s)", method, endpoint, tokenDisplay)

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
		if p.RefreshToken != "" {
			log.Printf("[Poller] 401 Unauthorized for trigger=%s. Attempting token refresh...", p.TriggerID)
			newTokens, err := RefreshOAuth2Token(p.Provider, p.RefreshToken)
			if err != nil {
				return nil, fmt.Errorf("token refresh failed: %w", err)
			}

			p.mu.Lock()
			p.Token = newTokens.AccessToken
			p.RefreshToken = newTokens.RefreshToken
			p.Expiry = time.Now().Add(time.Duration(newTokens.ExpiresIn) * time.Second)
			p.mu.Unlock()

			log.Printf("[Poller] Token refreshed successfully for trigger=%s. Retrying request...", p.TriggerID)
			// Retry the request once
			return p.doRequest(method, endpoint)
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

func (p *Poller) poll() {
	log.Printf("[Poller] Polling YouTube for trigger=%s capability=%s since=%s", p.TriggerID, p.CapabilityKey, p.lastCheck.Format(time.RFC3339))

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
		log.Printf("[Poller] Error polling %s: %v", p.CapabilityKey, err)
		return
	}

	newCheckTime := time.Now().UTC()
	for _, item := range items {
		p.fireEvent(item)
	}
	p.lastCheck = newCheckTime
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
		log.Printf("[Poller] Failed to publish event trigger=%s: %v", p.TriggerID, err)
	} else {
		log.Printf("[Poller] Fired event capability=%s trigger=%s", p.CapabilityKey, p.TriggerID)
	}
}

func (p *Poller) pollSearch() ([]map[string]interface{}, error) {
	query, _ := p.Config["search_query"].(string)
	endpoint := fmt.Sprintf("%s/search?part=snippet&q=%s&type=video&order=date&publishedAfter=%s",
		youtubeAPIBase, url.QueryEscape(query), url.QueryEscape(p.lastCheck.Format(time.RFC3339)))

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		id, _ := v["id"].(map[string]interface{})
		videoID, _ := id["videoId"].(string)

		results = append(results, map[string]interface{}{
			"title":        snippet["title"],
			"description":  snippet["description"],
			"url":          "https://www.youtube.com/watch?v=" + videoID,
			"author_name":  snippet["channelTitle"],
			"published_at": snippet["publishedAt"],
		})
	}
	return results, nil
}

func (p *Poller) pollLikedVideos() ([]map[string]interface{}, error) {
	// Reverted to videos endpoint as activities endpoint caused 403 Forbidden for user
	endpoint := fmt.Sprintf("%s/videos?part=snippet&myRating=like&maxResults=50", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		videoID, _ := v["id"].(string)
		if videoID == "" {
			continue
		}

		// If this is the first poll, we just seed the seenIDs map without firing events
		if p.isFirstPoll {
			p.seenIDs[videoID] = true
			continue
		}

		// If we haven't seen this video ID before, it's a new like
		if !p.seenIDs[videoID] {
			snippet, _ := v["snippet"].(map[string]interface{})
			p.seenIDs[videoID] = true

			results = append(results, map[string]interface{}{
				"title":       snippet["title"],
				"description": snippet["description"],
				"url":         "https://www.youtube.com/watch?v=" + videoID,
				"author_name": snippet["channelTitle"],
				"liked_at":    time.Now().UTC().Format(time.RFC3339), // Approximate since videos.list lacks liked-at
			})
		}
	}

	if p.isFirstPoll {
		log.Printf("[Poller] Seeded %d liked video IDs for trigger=%s", len(p.seenIDs), p.TriggerID)
		p.isFirstPoll = false
	}

	return results, nil
}

func (p *Poller) pollSubscriptions() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/subscriptions?part=snippet&mine=true&order=relevance", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		publishedAt, _ := snippet["publishedAt"].(string)
		t, _ := time.Parse(time.RFC3339, publishedAt)

		if t.After(p.lastCheck) {
			resID, _ := snippet["resourceId"].(map[string]interface{})
			channelID, _ := resID["channelId"].(string)
			results = append(results, map[string]interface{}{
				"channel_name": snippet["title"],
				"channel_url":  "https://www.youtube.com/channel/" + channelID,
				"description":  snippet["description"],
			})
		}
	}
	return results, nil
}

func (p *Poller) pollChannelVideos() ([]map[string]interface{}, error) {
	channelID, _ := p.Config["channel_name_or_id"].(string)
	endpoint := fmt.Sprintf("%s/search?part=snippet&channelId=%s&type=video&order=date&publishedAfter=%s",
		youtubeAPIBase, url.QueryEscape(channelID), url.QueryEscape(p.lastCheck.Format(time.RFC3339)))

	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		id, _ := v["id"].(map[string]interface{})
		videoID, _ := id["videoId"].(string)

		results = append(results, map[string]interface{}{
			"title":        snippet["title"],
			"description":  snippet["description"],
			"url":          "https://www.youtube.com/watch?v=" + videoID,
			"author_name":  snippet["channelTitle"],
			"published_at": snippet["publishedAt"],
		})
	}
	return results, nil
}

func (p *Poller) pollPlaylists() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/playlists?part=snippet&mine=true", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		publishedAt, _ := snippet["publishedAt"].(string)
		t, _ := time.Parse(time.RFC3339, publishedAt)

		if t.After(p.lastCheck) {
			results = append(results, map[string]interface{}{
				"title":        snippet["title"],
				"description":  snippet["description"],
				"url":          "https://www.youtube.com/playlist?list=" + v["id"].(string),
				"published_at": publishedAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollMyUploads() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/activities?part=snippet,contentDetails&mine=true", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		if snippet["type"] != "upload" {
			continue
		}

		publishedAt, _ := snippet["publishedAt"].(string)
		t, _ := time.Parse(time.RFC3339, publishedAt)

		if t.After(p.lastCheck) {
			details, _ := v["contentDetails"].(map[string]interface{})
			upload, _ := details["upload"].(map[string]interface{})
			videoID, _ := upload["videoId"].(string)

			results = append(results, map[string]interface{}{
				"title":        snippet["title"],
				"description":  snippet["description"],
				"url":          "https://www.youtube.com/watch?v=" + videoID,
				"embed_code":   fmt.Sprintf("<iframe width=\"560\" height=\"315\" src=\"https://www.youtube.com/embed/%s\" frameborder=\"0\" allowfullscreen></iframe>", videoID),
				"published_at": publishedAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollSubscriptionActivities() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/activities?part=snippet,contentDetails&home=true", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		if snippet["type"] != "upload" {
			continue
		}

		publishedAt, _ := snippet["publishedAt"].(string)
		t, _ := time.Parse(time.RFC3339, publishedAt)

		if t.After(p.lastCheck) {
			details, _ := v["contentDetails"].(map[string]interface{})
			upload, _ := details["upload"].(map[string]interface{})
			videoID, _ := upload["videoId"].(string)

			results = append(results, map[string]interface{}{
				"title":        snippet["title"],
				"description":  snippet["description"],
				"url":          "https://www.youtube.com/watch?v=" + videoID,
				"author_name":  snippet["channelTitle"],
				"published_at": publishedAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollSuperChat() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/superChatEvents?part=snippet", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		createdAt, _ := snippet["createdAt"].(string)
		t, _ := time.Parse(time.RFC3339, createdAt)

		if t.After(p.lastCheck) {
			results = append(results, map[string]interface{}{
				"message":      snippet["commentText"],
				"author_name":  snippet["supporterDetails"].(map[string]interface{})["displayName"],
				"amount":       fmt.Sprintf("%v", snippet["amountMicros"].(float64)/1000000),
				"currency":     snippet["currency"],
				"published_at": createdAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollMemberships() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/members?part=snippet", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		// members endpoint requires a channelId of a channel you own
		return nil, nil // Silently skip if not applicable or unauthorized
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		joinedAt, _ := snippet["memberSince"].(string)
		t, _ := time.Parse(time.RFC3339, joinedAt)

		if t.After(p.lastCheck) {
			details, _ := snippet["memberDetails"].(map[string]interface{})
			results = append(results, map[string]interface{}{
				"member_name": details["displayName"],
				"level":       snippet["membershipsDetails"].(map[string]interface{})["highestAccessibleLevel"],
				"joined_at":   joinedAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollSuperStickers() ([]map[string]interface{}, error) {
	// Super Stickers are often included in Super Chat events or have a similar structure
	endpoint := fmt.Sprintf("%s/superChatEvents?part=snippet", youtubeAPIBase)
	resp, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, _ := resp["items"].([]interface{})
	for _, item := range items {
		v, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		snippet, _ := v["snippet"].(map[string]interface{})
		if snippet["isSuperStickerEvent"] != true {
			continue
		}

		createdAt, _ := snippet["createdAt"].(string)
		t, _ := time.Parse(time.RFC3339, createdAt)

		if t.After(p.lastCheck) {
			sticker, _ := snippet["superStickerMetadata"].(map[string]interface{})
			results = append(results, map[string]interface{}{
				"sticker_url":  sticker["stickerId"], // Simplified
				"author_name":  snippet["supporterDetails"].(map[string]interface{})["displayName"],
				"amount":       fmt.Sprintf("%v", snippet["amountMicros"].(float64)/1000000),
				"currency":     snippet["currency"],
				"published_at": createdAt,
			})
		}
	}
	return results, nil
}
