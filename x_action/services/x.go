package services

import (
	"bytes"
	"encoding/base64"
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

	"x_action/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ── X (Twitter) API v2 ───────────────────────────────────────────────────────
//
// All tweet creation uses the v2 API:
//   POST https://api.x.com/2/tweets
//   Authorization: Bearer <user_access_token>
//   Content-Type: application/json
//   Body: {"text": "Hello world"}
//
// For tweets with images, a two-step flow is required:
//   1. Upload the image via the v1.1 media upload endpoint:
//      POST https://upload.twitter.com/1.1/media/upload.json
//      (multipart/form-data with media_data as base64-encoded image)
//   2. Attach the returned media_id_string to the tweet:
//      POST https://api.x.com/2/tweets
//      Body: {"text": "Hello", "media": {"media_ids": ["<media_id>"]}}
//
// Authentication: OAuth 2.0 Authorization Code Flow with PKCE (user context).
// Required scopes: tweet.write, tweet.read, users.read, offline.access
//
// Token refresh endpoint:
//   POST https://api.x.com/2/oauth2/token
//   grant_type=refresh_token&refresh_token=<token>&client_id=<id>
//
// Rate limits (Free tier):
//   - POST /2/tweets: 17 requests per 24 hours per user (Free),
//     100/day (Basic $100/mo), 500K/mo (Pro $5000/mo).
//   - Media upload: 415 requests per 15-minute window.
//
// NOTE: The Free tier is extremely limited (17 tweets/day per user).
//       Basic tier ($100/month) allows 100 tweets/day per app.

const (
	xAPIBase       = "https://api.x.com/2"
	xMediaUpload   = "https://upload.twitter.com/1.1/media/upload.json"
	xTokenEndpoint = "https://api.x.com/2/oauth2/token"
)

// ── XService ─────────────────────────────────────────────────────────────────

type XService struct {
	httpClient *http.Client
}

func NewXService() *XService {
	return &XService{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// ConfigProvider defines an interface to retrieve an ActionConfig by ID.
type ConfigProvider interface {
	GetConfig(id string) (models.ActionConfig, error)
	UpdateAuth(id string, auth map[string]models.AuthData) error
}

// PublisherProvider defines an interface to publish ActionResults.
type PublisherProvider interface {
	Publish(workflowID string, result models.ActionResult) error
}

// ── Auth helpers ─────────────────────────────────────────────────────────────

func getAuth(ctx map[string]models.AuthData) models.AuthData {
	// Try known X/Twitter provider keys first
	targets := []string{"x", "twitter", "x-oauth2", "twitter-oauth2", "x.com"}
	for _, t := range targets {
		if a, ok := ctx[t]; ok && a.AccessToken != "" {
			return a
		}
	}
	// Fallback: first entry with a token
	for _, a := range ctx {
		if a.AccessToken != "" {
			return a
		}
	}
	return models.AuthData{}
}

// ── Template resolution ──────────────────────────────────────────────────────

var templatePattern = regexp.MustCompile(`\{\{trigger\.payload\.(\w+)\}\}`)

// resolveTemplates replaces {{trigger.payload.X}} templates in config fields
// with corresponding values from the trigger event payload.
func resolveTemplates(cfg *models.ActionConfig, payload map[string]interface{}) {
	if payload == nil || cfg.RawConfig == nil {
		return
	}

	// Deep-copy RawConfig to avoid mutating stored templates.
	resolved := make(map[string]interface{}, len(cfg.RawConfig))
	for k, v := range cfg.RawConfig {
		resolved[k] = v
	}
	cfg.RawConfig = resolved

	resolveString := func(s string) string {
		if !strings.Contains(s, "{{trigger.payload.") {
			return s
		}
		return templatePattern.ReplaceAllStringFunc(s, func(match string) string {
			submatches := templatePattern.FindStringSubmatch(match)
			if len(submatches) < 2 {
				return match
			}
			key := submatches[1]
			if val, ok := payload[key]; ok {
				return fmt.Sprintf("%v", val)
			}
			log.Printf("[resolveTemplates] WARNING: payload missing key '%s', leaving template unreplaced", key)
			return match
		})
	}

	for k, v := range cfg.RawConfig {
		if s, ok := v.(string); ok {
			cfg.RawConfig[k] = resolveString(s)
		}
	}

	// Re-extract typed fields from resolved raw config
	if v, ok := cfg.RawConfig["tweet_text"].(string); ok {
		cfg.TweetText = v
	}
	if v, ok := cfg.RawConfig["image_url"].(string); ok {
		cfg.ImageURL = v
	}
}

// ── Result publishing ────────────────────────────────────────────────────────

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

// ── Task router ──────────────────────────────────────────────────────────────

// HandleTaskRouter returns a RabbitMQ delivery handler that routes to the
// correct capability method. Each consumer instance keeps its own copy of
// the config for independent auth state.
func (s *XService) HandleTaskRouter(cfgProvider ConfigProvider, publisher PublisherProvider, seq uint64, instanceID string, initialCfg models.ActionConfig) func(amqp.Delivery) {
	currentCfg := initialCfg

	return func(d amqp.Delivery) {
		var task models.ActionTask
		if err := json.Unmarshal(d.Body, &task); err != nil {
			log.Printf("[Consumer #%d] Error unmarshaling task: %v", seq, err)
			d.Nack(false, false)
			return
		}

		log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Received task: %s", seq, task.WorkflowID, instanceID, task.CapabilityKey)

		// Resolve templates in a COPY to avoid mutating the base config.
		taskCfg := currentCfg
		resolveTemplates(&taskCfg, task.Payload)

		log.Printf("[Consumer #%d] [Workflow: %s] [Instance: %s] Resolved config: tweet_text=%q image_url=%q",
			seq, task.WorkflowID, instanceID, taskCfg.TweetText, taskCfg.ImageURL)

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
		case "post_a_tweet":
			resultOutput, elapsedMs, procErr = s.PostTweet(auth, taskCfg)
		case "post_a_tweet_with_image":
			resultOutput, elapsedMs, procErr = s.PostTweetWithImage(auth, taskCfg)
		default:
			log.Printf("Unknown capability key: %s", capability)
			d.Nack(false, false)
			return
		}

		// Retry once on 401 Unauthorized
		if procErr != nil && strings.Contains(procErr.Error(), "401") {
			log.Printf("[XService] Action failed with 401, trying immediate refresh and retry for instance %s", instanceID)
			newAuth, refreshErr := s.RefreshAccessToken(auth)
			if refreshErr == nil {
				for k := range currentCfg.AuthContext {
					currentCfg.AuthContext[k] = newAuth
					break
				}
				_ = cfgProvider.UpdateAuth(instanceID, currentCfg.AuthContext)

				switch capability {
				case "post_a_tweet":
					resultOutput, elapsedMs, procErr = s.PostTweet(newAuth, taskCfg)
				case "post_a_tweet_with_image":
					resultOutput, elapsedMs, procErr = s.PostTweetWithImage(newAuth, taskCfg)
				}
			} else {
				log.Printf("[XService] Refresh failed during retry: %v", refreshErr)
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

// ── Auth management ──────────────────────────────────────────────────────────

// GetValidAuth checks if the token is expired and refreshes it if necessary.
func (s *XService) GetValidAuth(id string, cfgProvider ConfigProvider, cfg *models.ActionConfig) (models.AuthData, error) {
	auth := getAuth(cfg.AuthContext)
	if auth.AccessToken == "" {
		return models.AuthData{}, fmt.Errorf("no auth data found")
	}

	// X OAuth 2.0 access tokens expire after 2 hours.
	// Refresh proactively if within 5 minutes of expiry.
	if !auth.Expiry.IsZero() && time.Now().Add(5*time.Minute).After(auth.Expiry) {
		log.Printf("[XService] Token expired or expiring soon for action %s, refreshing...", id)
		newAuth, err := s.RefreshAccessToken(auth)
		if err != nil {
			return models.AuthData{}, fmt.Errorf("refresh failed: %w", err)
		}

		for k := range cfg.AuthContext {
			cfg.AuthContext[k] = newAuth
			break
		}
		if err := cfgProvider.UpdateAuth(id, cfg.AuthContext); err != nil {
			log.Printf("[XService] Warning: failed to update auth cache: %v", err)
		}
		return newAuth, nil
	}

	return auth, nil
}

// RefreshAccessToken uses the X OAuth 2.0 token endpoint to get a new
// access token using the stored refresh token.
//
// Endpoint: POST https://api.x.com/2/oauth2/token
// Body: grant_type=refresh_token&refresh_token=<token>&client_id=<id>
//
// X OAuth 2.0 tokens expire after 2 hours. Refresh tokens are valid for
// 6 months and are rotated on each refresh (a new refresh_token is returned).
func (s *XService) RefreshAccessToken(auth models.AuthData) (models.AuthData, error) {
	clientID := os.Getenv("X_CLIENT_ID")
	if clientID == "" {
		clientID = os.Getenv("TWITTER_CLIENT_ID")
	}
	clientSecret := os.Getenv("X_CLIENT_SECRET")
	if clientSecret == "" {
		clientSecret = os.Getenv("TWITTER_CLIENT_SECRET")
	}

	if clientID == "" {
		return models.AuthData{}, fmt.Errorf("X_CLIENT_ID (or TWITTER_CLIENT_ID) not set")
	}

	if auth.RefreshToken == "" {
		return models.AuthData{}, fmt.Errorf("no refresh token available — user must re-authorize")
	}

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", auth.RefreshToken)
	data.Set("client_id", clientID)

	req, err := http.NewRequest("POST", xTokenEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return models.AuthData{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// If the app is a confidential client (has a client secret), use Basic Auth.
	if clientSecret != "" {
		req.SetBasicAuth(url.QueryEscape(clientID), url.QueryEscape(clientSecret))
	}

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
		return models.AuthData{}, fmt.Errorf("token refresh failed with status %d: %s", resp.StatusCode, string(body))
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

	auth.AccessToken = tokenResp.AccessToken
	// X rotates refresh tokens on each refresh — always save the new one.
	if tokenResp.RefreshToken != "" {
		auth.RefreshToken = tokenResp.RefreshToken
	}
	auth.Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)

	log.Printf("[XService] Token refreshed successfully. Scopes: [%s], New expiry: %v", tokenResp.Scope, auth.Expiry)
	return auth, nil
}

// ── HTTP request helper ──────────────────────────────────────────────────────

func (s *XService) doRequest(method, endpoint string, auth models.AuthData, body []byte) (map[string]interface{}, int64, error) {
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

	log.Printf("[XService] Req: %s %s", method, endpoint)
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
	log.Printf("[XService] Resp: %d", resp.StatusCode)

	var result map[string]interface{}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			result = map[string]interface{}{"raw": string(respBody)}
		}
	} else {
		result = map[string]interface{}{"status": "success"}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errMsg := "API error"
		if detail, ok := result["detail"].(string); ok {
			errMsg = detail
		} else if title, ok := result["title"].(string); ok {
			errMsg = title
		} else if strings.TrimSpace(string(respBody)) != "" {
			errMsg = string(respBody)
		}

		if resp.StatusCode == 403 {
			log.Printf("[XService] 403 Forbidden: Check that your app has 'Read and Write' permissions and the user authorized with tweet.write scope.")
		}
		if resp.StatusCode == 429 {
			log.Printf("[XService] 429 Rate Limited: Free tier allows only 17 tweets/day per user. Consider upgrading to Basic ($100/mo) for 100/day.")
		}

		return nil, elapsed, fmt.Errorf("x api returned %d: %s", resp.StatusCode, errMsg)
	}

	return result, elapsed, nil
}

// ── Capability: Post a Tweet ─────────────────────────────────────────────────

// PostTweet posts a text-only tweet to the authenticated user's timeline.
//
// Endpoint: POST https://api.x.com/2/tweets
// Auth: OAuth 2.0 User Context (Bearer access token)
// Body: {"text": "<tweet_text>"}
//
// Rate limit: 17 tweets/24h (Free), 100/day (Basic $100/mo).
func (s *XService) PostTweet(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	tweetText := cfg.TweetText
	if tweetText == "" {
		return nil, 0, fmt.Errorf("tweet_text is required for post_a_tweet")
	}

	payload := map[string]interface{}{
		"text": tweetText,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal tweet payload: %w", err)
	}

	endpoint := xAPIBase + "/tweets"
	return s.doRequest("POST", endpoint, auth, body)
}

// ── Capability: Post a Tweet with Image ──────────────────────────────────────

// PostTweetWithImage posts a tweet with an attached image.
//
// This is a two-step process:
//   1. Download the image from image_url, then upload it to X via the
//      v1.1 media upload endpoint (POST https://upload.twitter.com/1.1/media/upload.json)
//      using multipart/form-data with the image as base64-encoded media_data.
//   2. Create the tweet via v2 API with the returned media_id attached.
//
// NOTE: The v1.1 media upload endpoint is the only way to upload images.
//       X has not yet fully migrated media upload to v2.
//       The same OAuth 2.0 Bearer token works for both endpoints as long as
//       the user authorized with tweet.write scope.
//
// Rate limit: Media upload — 415 requests per 15-minute window.
//             Tweet creation — 17/24h (Free), 100/day (Basic $100/mo).
func (s *XService) PostTweetWithImage(auth models.AuthData, cfg models.ActionConfig) (map[string]interface{}, int64, error) {
	tweetText := cfg.TweetText
	if tweetText == "" {
		return nil, 0, fmt.Errorf("tweet_text is required for post_a_tweet_with_image")
	}

	imageURL := cfg.ImageURL
	if imageURL == "" {
		return nil, 0, fmt.Errorf("image_url is required for post_a_tweet_with_image")
	}

	start := time.Now()

	// Step 1: Download the image from the provided URL.
	imgResp, err := s.httpClient.Get(imageURL)
	if err != nil {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("downloading image from %s: %w", imageURL, err)
	}
	defer imgResp.Body.Close()

	if imgResp.StatusCode < 200 || imgResp.StatusCode >= 300 {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("image download returned HTTP %d", imgResp.StatusCode)
	}

	imgBytes, err := io.ReadAll(imgResp.Body)
	if err != nil {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("reading image bytes: %w", err)
	}

	if len(imgBytes) == 0 {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("downloaded image is empty")
	}

	// Image must be under 5MB for simple upload.
	if len(imgBytes) > 5*1024*1024 {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("image exceeds 5MB limit (%d bytes)", len(imgBytes))
	}

	// Step 2: Upload image to X via v1.1 media upload endpoint.
	mediaID, err := s.uploadMedia(auth, imgBytes)
	if err != nil {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("uploading media: %w", err)
	}

	// Step 3: Create tweet with the attached media_id.
	payload := map[string]interface{}{
		"text": tweetText,
		"media": map[string]interface{}{
			"media_ids": []string{mediaID},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		elapsed := time.Since(start).Milliseconds()
		return nil, elapsed, fmt.Errorf("marshal tweet payload: %w", err)
	}

	endpoint := xAPIBase + "/tweets"
	return s.doRequest("POST", endpoint, auth, body)
}

// uploadMedia uploads an image to X using the v1.1 media upload endpoint.
// It sends the image as base64-encoded media_data in a POST form request.
//
// Endpoint: POST https://upload.twitter.com/1.1/media/upload.json
// Auth: Bearer <access_token>
// Body: media_data=<base64_encoded_image> (application/x-www-form-urlencoded)
//
// Returns the media_id_string on success.
func (s *XService) uploadMedia(auth models.AuthData, imgBytes []byte) (string, error) {
	encoded := base64.StdEncoding.EncodeToString(imgBytes)

	data := url.Values{}
	data.Set("media_data", encoded)

	req, err := http.NewRequest("POST", xMediaUpload, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("creating media upload request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("media upload request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading media upload response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("media upload returned %d: %s", resp.StatusCode, string(respBody))
	}

	var uploadResp struct {
		MediaIDString string `json:"media_id_string"`
	}
	if err := json.Unmarshal(respBody, &uploadResp); err != nil {
		return "", fmt.Errorf("parsing media upload response: %w", err)
	}

	if uploadResp.MediaIDString == "" {
		return "", fmt.Errorf("media upload returned empty media_id_string")
	}

	log.Printf("[XService] Media uploaded successfully, media_id=%s", uploadResp.MediaIDString)
	return uploadResp.MediaIDString, nil
}
