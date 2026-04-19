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
	"sync"
	"time"

	"github.com/google/uuid"
	"github_trigger/models"
	"github_trigger/worker"
)

const githubAPIBase = "https://api.github.com"

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
	SequenceNumber uint64
	Publisher      *worker.Publisher
	httpClient     *http.Client
	stopChan       chan struct{}
	lastCheck      time.Time // kept for legacy capabilities that still use time-based filtering
	mu             sync.Mutex

	// Ordered cache for change detection.
	// seenCache is a slice of IDs (oldest → newest); seenSet is the O(1) lookup mirror.
	seenCache   []string
	seenSet     map[string]bool
	isFirstPoll bool
}

func NewPoller(triggerID, workflowID string, config models.TriggerConfig, seq uint64, pub *worker.Publisher) *Poller {
	// Extract auth
	var auth models.AuthData
	targets := []string{"github", "github-oauth2"}
	for _, target := range targets {
		if a, ok := config.AuthContext[target]; ok && a.AccessToken != "" {
			auth = a
			auth.Provider = target
			break
		}
	}

	// Fallback: If no specific provider found, take the first one with an access token
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
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		stopChan:       make(chan struct{}),
		lastCheck:      time.Now().UTC(),
		seenCache:      make([]string, 0, 100),
		seenSet:        make(map[string]bool),
		isFirstPoll:    true,
	}
}

// RefreshOAuth2Token handles refreshing tokens for GitHub.
func RefreshOAuth2Token(refreshToken string) (string, string, int, error) {
	tokenURL := "https://github.com/login/oauth/access_token"
	clientID := os.Getenv("GITHUB_CLIENT_ID")
	clientSecret := os.Getenv("GITHUB_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		return "", "", 0, fmt.Errorf("GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET not set")
	}

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", refreshToken)
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)

	req, err := http.NewRequest("POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", 0, err
	}

	if resp.StatusCode != http.StatusOK {
		return "", "", 0, fmt.Errorf("refresh failed: %s", string(body))
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", "", 0, err
	}

	return tokenResp.AccessToken, tokenResp.RefreshToken, tokenResp.ExpiresIn, nil
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
func (p *Poller) doRequest(method, endpoint string) (interface{}, error) {
	return p.executeRequest(method, endpoint, true)
}

// executeRequest performs the HTTP call. canRetry guards against infinite
// recursion when a refreshed token still returns 401.
func (p *Poller) executeRequest(method, endpoint string, canRetry bool) (interface{}, error) {
	req, err := http.NewRequest(method, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+p.Token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "Goat-Automate-GitHub-Plugin-v1")

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
			log.Printf("[Poller #%d] [Workflow: %s] 401 Unauthorized for GitHub trigger=%s. Attempting token refresh...", p.SequenceNumber, p.WorkflowID, p.TriggerID)
			newAccessToken, newRefreshToken, expiresIn, err := RefreshOAuth2Token(p.RefreshToken)
			if err == nil {
				p.Token = newAccessToken
				if newRefreshToken != "" {
					p.RefreshToken = newRefreshToken
				}
				if expiresIn > 0 {
					p.Expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
				}
				log.Printf("[Poller #%d] [Workflow: %s] Token refreshed successfully for GitHub trigger=%s. Retrying...", p.SequenceNumber, p.WorkflowID, p.TriggerID)
				// Retry once — canRetry=false prevents infinite recursion
				return p.executeRequest(method, endpoint, false)
			}
			log.Printf("[Poller #%d] [Workflow: %s] GitHub token refresh failed: %v", p.SequenceNumber, p.WorkflowID, err)
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("api returned %d: %s", resp.StatusCode, string(body))
	}

	var result interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshaling response: %w", err)
	}

	return result, nil
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
	log.Printf("[Poller #%d] [Workflow: %s] Polling GitHub for trigger=%s capability=%s",
		p.SequenceNumber, p.WorkflowID, p.TriggerID, p.CapabilityKey)

	var items []map[string]interface{}
	var err error

	switch p.CapabilityKey {
	// ── Perfected capabilities (cache-based dedup) ──────────────────────
	case "github_new_issue":
		items, err = p.pollRepoIssues()
	case "github_new_closed_issue":
		items, err = p.pollRepoClosedIssues()
	case "github_new_issue_assigned_to_you":
		items, err = p.pollAssignedIssues()
	case "github_new_repository_by_username_or_org":
		items, err = p.pollUserRepos()
	case "github_new_pull_request":
		items, err = p.pollPullRequests()
	// ── Legacy capabilities (kept working, use time-based filtering) ────
	case "github_new_notification_from_repo":
		items, err = p.pollRepoNotifications()
	case "github_new_repository_event":
		items, err = p.pollRepoEvents()
	case "github_new_release":
		items, err = p.pollReleases()
	case "github_new_commit":
		items, err = p.pollCommits()
	case "github_new_notification":
		items, err = p.pollAllNotifications()
	case "github_new_gist":
		items, err = p.pollGists()
	default:
		log.Printf("[Poller] Unknown capability: %s", p.CapabilityKey)
		return
	}

	if err != nil {
		log.Printf("[Poller #%d] [Workflow: %s] Error polling %s: %v", p.SequenceNumber, p.WorkflowID, p.CapabilityKey, err)
		return
	}

	// For the 5 perfected capabilities use the cache-based dedup path.
	if p.usesCacheDedup() {
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
		return
	}

	// Legacy path — fire all items and advance lastCheck.
	newCheckTime := time.Now().UTC()
	for _, item := range items {
		p.fireEvent(item)
	}
	p.lastCheck = newCheckTime
}

// usesCacheDedup returns true for the 5 perfected capabilities.
func (p *Poller) usesCacheDedup() bool {
	switch p.CapabilityKey {
	case "github_new_issue",
		"github_new_closed_issue",
		"github_new_issue_assigned_to_you",
		"github_new_repository_by_username_or_org",
		"github_new_pull_request":
		return true
	}
	return false
}

func (p *Poller) fireEvent(payload map[string]interface{}) {
	event := models.TriggerEvent{
		ID:            uuid.New().String(),
		WorkflowID:    p.WorkflowID,
		TriggerID:     p.TriggerID,
		Type:          "event",
		Name:          "GitHub Trigger",
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

// ── Repository URL helper ────────────────────────────────────────────────────

// repoHTMLURL builds the GitHub HTML URL for a given "owner/repo" string.
func repoHTMLURL(ownerSlashRepo string) string {
	if ownerSlashRepo == "" {
		return ""
	}
	return "https://github.com/" + ownerSlashRepo
}

// ── Perfected capability poll functions ──────────────────────────────────────

// pollRepoIssues lists open issues for the configured repository.
// Endpoint: GET /repos/{owner}/{repo}/issues?state=open&sort=created&direction=desc&per_page=50
// GitHub REST API: free, no payment required. Rate limit: 5000 req/hr (authenticated).
//
// NOTE: The GitHub API considers every pull request an issue. We filter them out
// by skipping items that have a "pull_request" key.
func (p *Poller) pollRepoIssues() ([]map[string]interface{}, error) {
	repo := p.TriggerConfig.Repository
	if repo == "" {
		return nil, fmt.Errorf("repository is required for github_new_issue")
	}

	endpoint := fmt.Sprintf("%s/repos/%s/issues?state=open&sort=created&direction=desc&per_page=50",
		githubAPIBase, repo)

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		// Skip pull requests (GitHub returns PRs in the issues endpoint)
		if v["pull_request"] != nil {
			continue
		}

		issueNumber := fmt.Sprintf("%v", v["number"])
		if issueNumber == "" || issueNumber == "<nil>" {
			continue
		}

		user := safeMap(v["user"])
		repoObj := safeMap(v["repository"])
		repoURL := safeString(repoObj["html_url"])
		if repoURL == "" {
			repoURL = repoHTMLURL(repo)
		}

		results = append(results, map[string]interface{}{
			"_id":            issueNumber,
			"issue_title":    safeString(v["title"]),
			"issue_body":     safeString(v["body"]),
			"issue_url":      safeString(v["html_url"]),
			"user":           safeString(user["login"]),
			"created_at":     safeString(v["created_at"]),
			"repository_url": repoURL,
		})
	}
	return results, nil
}

// pollRepoClosedIssues lists recently closed issues for the configured repository.
// Endpoint: GET /repos/{owner}/{repo}/issues?state=closed&sort=updated&direction=desc&per_page=50
// GitHub REST API: free, no payment required.
func (p *Poller) pollRepoClosedIssues() ([]map[string]interface{}, error) {
	repo := p.TriggerConfig.Repository
	if repo == "" {
		return nil, fmt.Errorf("repository is required for github_new_closed_issue")
	}

	endpoint := fmt.Sprintf("%s/repos/%s/issues?state=closed&sort=updated&direction=desc&per_page=50",
		githubAPIBase, repo)

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		// Skip pull requests
		if v["pull_request"] != nil {
			continue
		}

		issueNumber := fmt.Sprintf("%v", v["number"])
		if issueNumber == "" || issueNumber == "<nil>" {
			continue
		}

		user := safeMap(v["user"])
		repoObj := safeMap(v["repository"])
		repoURL := safeString(repoObj["html_url"])
		if repoURL == "" {
			repoURL = repoHTMLURL(repo)
		}

		results = append(results, map[string]interface{}{
			"_id":            issueNumber,
			"issue_title":    safeString(v["title"]),
			"issue_body":     safeString(v["body"]),
			"issue_url":      safeString(v["html_url"]),
			"user":           safeString(user["login"]),
			"closed_at":      safeString(v["closed_at"]),
			"repository_url": repoURL,
		})
	}
	return results, nil
}

// pollAssignedIssues lists issues assigned to the authenticated user.
// Endpoint: GET /user/issues?filter=assigned&state=open&sort=created&direction=desc&per_page=50
// GitHub REST API: free, no payment required.
func (p *Poller) pollAssignedIssues() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/user/issues?filter=assigned&state=open&sort=created&direction=desc&per_page=50",
		githubAPIBase)

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		// Skip pull requests
		if v["pull_request"] != nil {
			continue
		}

		issueNumber := fmt.Sprintf("%v", v["number"])
		repoFullName := ""
		repoObj := safeMap(v["repository"])
		if repoObj != nil {
			repoFullName = safeString(repoObj["full_name"])
		}
		// Use a composite ID (repo + number) since issues come from multiple repos
		compositeID := repoFullName + "#" + issueNumber
		if issueNumber == "" || issueNumber == "<nil>" {
			continue
		}

		user := safeMap(v["user"])
		repoURL := safeString(repoObj["html_url"])

		results = append(results, map[string]interface{}{
			"_id":            compositeID,
			"issue_title":    safeString(v["title"]),
			"issue_body":     safeString(v["body"]),
			"issue_url":      safeString(v["html_url"]),
			"user":           safeString(user["login"]),
			"assigned_at":    safeString(v["updated_at"]), // GitHub does not expose assignment timestamp; updated_at is the closest proxy
			"repository_url": repoURL,
		})
	}
	return results, nil
}

// pollUserRepos lists repositories for a specific user or organization, newest first.
// Endpoint: GET /users/{username}/repos?sort=created&direction=desc&per_page=50
// Fallback: GET /orgs/{org}/repos?sort=created&direction=desc&per_page=50
// GitHub REST API: free, no payment required.
func (p *Poller) pollUserRepos() ([]map[string]interface{}, error) {
	userOrOrg := p.TriggerConfig.UsernameOrOrganization
	if userOrOrg == "" {
		return nil, fmt.Errorf("username or organization is required for github_new_repository_by_username_or_org")
	}

	// Try users first
	endpoint := fmt.Sprintf("%s/users/%s/repos?sort=created&direction=desc&per_page=50",
		githubAPIBase, userOrOrg)

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		// Fallback to orgs endpoint
		endpoint = fmt.Sprintf("%s/orgs/%s/repos?sort=created&direction=desc&per_page=50",
			githubAPIBase, userOrOrg)
		res, err = p.doRequest("GET", endpoint)
		if err != nil {
			return nil, err
		}
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}

		repoID := fmt.Sprintf("%v", v["id"])
		if repoID == "" || repoID == "<nil>" {
			continue
		}

		owner := safeMap(v["owner"])

		results = append(results, map[string]interface{}{
			"_id":             repoID,
			"repository_name": safeString(v["name"]),
			"description":     safeString(v["description"]),
			"url":             safeString(v["html_url"]),
			"owner":           safeString(owner["login"]),
			"created_at":      safeString(v["created_at"]),
			"repository_url":  safeString(v["html_url"]),
		})
	}
	return results, nil
}

// pollPullRequests lists open pull requests for the configured repository, newest first.
// Endpoint: GET /repos/{owner}/{repo}/pulls?state=open&sort=created&direction=desc&per_page=50
// GitHub REST API: free, no payment required.
func (p *Poller) pollPullRequests() ([]map[string]interface{}, error) {
	repo := p.TriggerConfig.Repository
	if repo == "" {
		return nil, fmt.Errorf("repository is required for github_new_pull_request")
	}

	endpoint := fmt.Sprintf("%s/repos/%s/pulls?state=open&sort=created&direction=desc&per_page=50",
		githubAPIBase, repo)

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}

		prNumber := fmt.Sprintf("%v", v["number"])
		if prNumber == "" || prNumber == "<nil>" {
			continue
		}

		user := safeMap(v["user"])

		results = append(results, map[string]interface{}{
			"_id":        prNumber,
			"title":      safeString(v["title"]),
			"body":       safeString(v["body"]),
			"url":        safeString(v["html_url"]),
			"user":       safeString(user["login"]),
			"created_at": safeString(v["created_at"]),
		})
	}
	return results, nil
}

// ── Legacy capability poll functions (kept for backward compatibility) ───────
// These use time-based filtering via p.lastCheck. They are NOT part of
// the perfected set but remain functional so nothing breaks.

func (p *Poller) pollRepoNotifications() ([]map[string]interface{}, error) {
	repo := p.TriggerConfig.Repository
	endpoint := fmt.Sprintf("%s/repos/%s/notifications?since=%s", githubAPIBase, repo, p.lastCheck.Format(time.RFC3339))

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		subject := safeMap(v["subject"])
		repository := safeMap(v["repository"])

		results = append(results, map[string]interface{}{
			"repository": safeString(repository["full_name"]),
			"title":      safeString(subject["title"]),
			"url":        safeString(subject["url"]),
			"type":       safeString(subject["type"]),
			"date":       safeString(v["updated_at"]),
		})
	}
	return results, nil
}

func (p *Poller) pollRepoEvents() ([]map[string]interface{}, error) {
	repo := p.TriggerConfig.Repository
	endpoint := fmt.Sprintf("%s/repos/%s/events", githubAPIBase, repo)

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		createdAt := safeString(v["created_at"])
		t, _ := time.Parse(time.RFC3339, createdAt)

		if t.After(p.lastCheck) {
			actor := safeMap(v["actor"])
			results = append(results, map[string]interface{}{
				"event_type": safeString(v["type"]),
				"repository": repo,
				"actor":      safeString(actor["login"]),
				"date":       createdAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollReleases() ([]map[string]interface{}, error) {
	repo := p.TriggerConfig.Repository
	endpoint := fmt.Sprintf("%s/repos/%s/releases", githubAPIBase, repo)

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		publishedAt := safeString(v["published_at"])
		t, _ := time.Parse(time.RFC3339, publishedAt)

		if t.After(p.lastCheck) {
			results = append(results, map[string]interface{}{
				"tag_name":     safeString(v["tag_name"]),
				"release_name": safeString(v["name"]),
				"body":         safeString(v["body"]),
				"published_at": publishedAt,
				"url":          safeString(v["html_url"]),
			})
		}
	}
	return results, nil
}

func (p *Poller) pollCommits() ([]map[string]interface{}, error) {
	repo := p.TriggerConfig.Repository
	endpoint := fmt.Sprintf("%s/repos/%s/commits?since=%s", githubAPIBase, repo, p.lastCheck.Format(time.RFC3339))

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		commit := safeMap(v["commit"])
		author := safeMap(commit["author"])

		results = append(results, map[string]interface{}{
			"message": safeString(commit["message"]),
			"author":  safeString(author["name"]),
			"url":     safeString(v["html_url"]),
			"date":    safeString(author["date"]),
			"sha":     safeString(v["sha"]),
		})
	}
	return results, nil
}

func (p *Poller) pollAllNotifications() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/notifications?since=%s", githubAPIBase, p.lastCheck.Format(time.RFC3339))

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		subject := safeMap(v["subject"])
		repository := safeMap(v["repository"])

		results = append(results, map[string]interface{}{
			"repository": safeString(repository["full_name"]),
			"title":      safeString(subject["title"]),
			"url":        safeString(subject["url"]),
			"type":       safeString(subject["type"]),
			"date":       safeString(v["updated_at"]),
		})
	}
	return results, nil
}

func (p *Poller) pollGists() ([]map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/gists?since=%s", githubAPIBase, p.lastCheck.Format(time.RFC3339))

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for _, item := range safeSlice(res) {
		v := safeMap(item)
		if v == nil {
			continue
		}
		owner := safeMap(v["owner"])

		results = append(results, map[string]interface{}{
			"description": safeString(v["description"]),
			"url":         safeString(v["html_url"]),
			"created_at":  safeString(v["created_at"]),
			"owner":       safeString(owner["login"]),
		})
	}
	return results, nil
}
