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

	"github.com/google/uuid"
	"github_trigger/models"
	"github_trigger/worker"
)

const githubAPIBase = "https://api.github.com"

type Poller struct {
	TriggerID     string
	WorkflowID    string
	CapabilityKey string
	Config        map[string]interface{}
	Token         string
	RefreshToken  string
	Expiry        time.Time
	Provider      string
	Publisher     *worker.Publisher
	httpClient    *http.Client
	stopChan      chan struct{}
	lastCheck     time.Time
}

func NewPoller(triggerID, workflowID, capabilityKey string, config map[string]interface{}, auth models.AuthData, pub *worker.Publisher) *Poller {
	return &Poller{
		TriggerID:     triggerID,
		WorkflowID:    workflowID,
		CapabilityKey: capabilityKey,
		Config:        config,
		Token:         auth.AccessToken,
		RefreshToken:  auth.RefreshToken,
		Expiry:        auth.Expiry,
		Provider:      auth.Provider,
		Publisher:     pub,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		stopChan:      make(chan struct{}),
		lastCheck:     time.Now().UTC(),
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

func (p *Poller) doRequest(method, endpoint string) (interface{}, error) {
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
		if p.RefreshToken != "" {
			log.Printf("[Poller] 401 Unauthorized for GitHub trigger=%s. Attempting token refresh...", p.TriggerID)
			newAccessToken, newRefreshToken, expiresIn, err := RefreshOAuth2Token(p.RefreshToken)
			if err == nil {
				p.Token = newAccessToken
				if newRefreshToken != "" {
					p.RefreshToken = newRefreshToken
				}
				if expiresIn > 0 {
					p.Expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
				}
				log.Printf("[Poller] Token refreshed successfully for GitHub trigger=%s. Retrying...", p.TriggerID)
				return p.doRequest(method, endpoint)
			}
			log.Printf("[Poller] GitHub token refresh failed: %v", err)
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

func (p *Poller) poll() {
	log.Printf("[Poller] Polling GitHub for trigger=%s capability=%s since=%s", p.TriggerID, p.CapabilityKey, p.lastCheck.Format(time.RFC3339))

	var items []map[string]interface{}
	var err error

	switch p.CapabilityKey {
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
	case "github_new_issue":
		items, err = p.pollIssues("all")
	case "github_new_closed_issue":
		items, err = p.pollIssues("closed")
	case "github_new_issue_assigned_to_you":
		items, err = p.pollIssues("assigned")
	case "github_new_repository_by_username_or_org":
		items, err = p.pollUserRepos()
	case "github_new_pull_request":
		items, err = p.pollPullRequests()
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
		Name:          "GitHub Trigger",
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

func (p *Poller) pollRepoNotifications() ([]map[string]interface{}, error) {
	repo, _ := p.Config["repository"].(string)
	endpoint := fmt.Sprintf("%s/repos/%s/notifications?since=%s", githubAPIBase, repo, p.lastCheck.Format(time.RFC3339))
	
	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		subject, _ := v["subject"].(map[string]interface{})
		repository, _ := v["repository"].(map[string]interface{})
		
		results = append(results, map[string]interface{}{
			"repository": repository["full_name"],
			"title":      subject["title"],
			"url":        subject["url"],
			"type":       subject["type"],
			"date":       v["updated_at"],
		})
	}
	return results, nil
}

func (p *Poller) pollRepoEvents() ([]map[string]interface{}, error) {
	repo, _ := p.Config["repository"].(string)
	endpoint := fmt.Sprintf("%s/repos/%s/events", githubAPIBase, repo)
	
	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		createdAt, _ := v["created_at"].(string)
		t, _ := time.Parse(time.RFC3339, createdAt)

		if t.After(p.lastCheck) {
			actor, _ := v["actor"].(map[string]interface{})
			results = append(results, map[string]interface{}{
				"event_type": v["type"],
				"repository": repo,
				"actor":      actor["login"],
				"date":       createdAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollReleases() ([]map[string]interface{}, error) {
	repo, _ := p.Config["repository"].(string)
	endpoint := fmt.Sprintf("%s/repos/%s/releases", githubAPIBase, repo)
	
	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		publishedAt, _ := v["published_at"].(string)
		t, _ := time.Parse(time.RFC3339, publishedAt)

		if t.After(p.lastCheck) {
			results = append(results, map[string]interface{}{
				"tag_name":     v["tag_name"],
				"release_name": v["name"],
				"body":         v["body"],
				"published_at": publishedAt,
				"url":          v["html_url"],
			})
		}
	}
	return results, nil
}

func (p *Poller) pollCommits() ([]map[string]interface{}, error) {
	repo, _ := p.Config["repository"].(string)
	endpoint := fmt.Sprintf("%s/repos/%s/commits?since=%s", githubAPIBase, repo, p.lastCheck.Format(time.RFC3339))
	
	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		commit, _ := v["commit"].(map[string]interface{})
		author, _ := commit["author"].(map[string]interface{})
		
		results = append(results, map[string]interface{}{
			"message": commit["message"],
			"author":  author["name"],
			"url":     v["html_url"],
			"date":    author["date"],
			"sha":     v["sha"],
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
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		subject, _ := v["subject"].(map[string]interface{})
		repository, _ := v["repository"].(map[string]interface{})
		
		results = append(results, map[string]interface{}{
			"repository": repository["full_name"],
			"title":      subject["title"],
			"url":        subject["url"],
			"type":       subject["type"],
			"date":       v["updated_at"],
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
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		owner, _ := v["owner"].(map[string]interface{})
		
		results = append(results, map[string]interface{}{
			"description": v["description"],
			"url":         v["html_url"],
			"created_at":  v["created_at"],
			"owner":       owner["login"],
		})
	}
	return results, nil
}

func (p *Poller) pollIssues(filter string) ([]map[string]interface{}, error) {
	var endpoint string
	switch filter {
	case "closed":
		endpoint = githubAPIBase + "/issues?state=closed&since=" + p.lastCheck.Format(time.RFC3339)
	case "assigned":
		endpoint = githubAPIBase + "/issues?filter=assigned&since=" + p.lastCheck.Format(time.RFC3339)
	default:
		endpoint = githubAPIBase + "/issues?since=" + p.lastCheck.Format(time.RFC3339)
	}

	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		user, _ := v["user"].(map[string]interface{})
		
		results = append(results, map[string]interface{}{
			"issue_title": v["title"],
			"issue_body":  v["body"],
			"issue_url":   v["html_url"],
			"user":        user["login"],
			"created_at":  v["created_at"],
			"closed_at":   v["closed_at"],
			"assigned_at": v["updated_at"], // Approximate for assigned
		})
	}
	return results, nil
}

func (p *Poller) pollUserRepos() ([]map[string]interface{}, error) {
	userOrOrg, _ := p.Config["username_or_organization"].(string)
	// Try users first, then orgs if that fails? Or detect based on type if known.
	// For now, assume users as primary.
	endpoint := fmt.Sprintf("%s/users/%s/repos?sort=created&direction=desc", githubAPIBase, userOrOrg)
	
	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		// Try orgs
		endpoint = fmt.Sprintf("%s/orgs/%s/repos?sort=created&direction=desc", githubAPIBase, userOrOrg)
		res, err = p.doRequest("GET", endpoint)
		if err != nil {
			return nil, err
		}
	}

	var results []map[string]interface{}
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		createdAt, _ := v["created_at"].(string)
		t, _ := time.Parse(time.RFC3339, createdAt)

		if t.After(p.lastCheck) {
			owner, _ := v["owner"].(map[string]interface{})
			results = append(results, map[string]interface{}{
				"repository_name": v["name"],
				"description":     v["description"],
				"url":             v["html_url"],
				"owner":           owner["login"],
				"created_at":      createdAt,
			})
		}
	}
	return results, nil
}

func (p *Poller) pollPullRequests() ([]map[string]interface{}, error) {
	repo, _ := p.Config["repository"].(string)
	endpoint := fmt.Sprintf("%s/repos/%s/pulls?state=open&sort=created&direction=desc", githubAPIBase, repo)
	
	res, err := p.doRequest("GET", endpoint)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	items, ok := res.([]interface{})
	if !ok { return nil, nil }

	for _, item := range items {
		v, _ := item.(map[string]interface{})
		createdAt, _ := v["created_at"].(string)
		t, _ := time.Parse(time.RFC3339, createdAt)

		if t.After(p.lastCheck) {
			user, _ := v["user"].(map[string]interface{})
			results = append(results, map[string]interface{}{
				"title":      v["title"],
				"body":       v["body"],
				"url":        v["html_url"],
				"user":       user["login"],
				"created_at": createdAt,
			})
		}
	}
	return results, nil
}

