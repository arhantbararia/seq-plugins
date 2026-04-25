package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github_trigger/models"
)

// RegistrationService handles self-registration with the workflow_executor.
type RegistrationService struct{}

func NewRegistrationService() *RegistrationService {
	return &RegistrationService{}
}

func (s *RegistrationService) Register() error {
	executorURL := os.Getenv("EXECUTOR_URL")
	if executorURL == "" {
		executorURL = "http://localhost:8082"
	}
	host := os.Getenv("PLUGIN_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PLUGIN_PORT")
	if port == "" {
		port = "8089"
	}

	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-github-trigger"
	} else {
		pluginID = host + ":" + port + "-github-trigger"
	}
	prefix := "/github/trigger"
	req := models.RegistrationRequest{
		ID:                    pluginID, // Docker sets the container ID as the HOSTNAME env var
		Name:                  "GitHub Trigger",
		ContainerType:         "trigger",
		PluginProviderService: "GitHub",
		PluginHost:            host,
		PluginPort:            port,
		AuthTypes:             []string{"Personal Access Token"},
		Endpoints: map[string]string{
			"setup":  prefix + "/setup",
			"remove": prefix + "/remove",
			"health": prefix + "/health",
		},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "github_new_notification_from_repo",
				Name:          "Any new notification from a repository",
				Description:   "Triggers when there is a new notification from a specific repository.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"repository": "string",
				},
				OutputSchema: map[string]interface{}{
					"repository": "string",
					"title":      "string",
					"url":        "string",
					"type":       "string",
					"date":       "string",
				},
			},
			{
				UniqueKey:     "github_new_repository_event",
				Name:          "Any new repository event",
				Description:   "Triggers when there is any new repository event.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"repository": "string",
				},
				OutputSchema: map[string]interface{}{
					"event_type": "string",
					"repository": "string",
					"actor":      "string",
					"date":       "string",
				},
			},
			{
				UniqueKey:     "github_new_release",
				Name:          "Any new release",
				Description:   "Triggers when there is a new release in a repository.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"repository": "string",
				},
				OutputSchema: map[string]interface{}{
					"tag_name":     "string",
					"release_name": "string",
					"body":         "string",
					"published_at": "string",
					"url":          "string",
				},
			},
			{
				UniqueKey:     "github_new_commit",
				Name:          "Any new commit",
				Description:   "Triggers when there is a new commit in a repository.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"repository": "string",
				},
				OutputSchema: map[string]interface{}{
					"message": "string",
					"author":  "string",
					"url":     "string",
					"date":    "string",
					"sha":     "string",
				},
			},
			{
				UniqueKey:     "github_new_notification",
				Name:          "Any new notification",
				Description:   "Triggers when there is any new notification.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"repository": "string",
					"title":      "string",
					"url":        "string",
					"type":       "string",
					"date":       "string",
				},
			},
			{
				UniqueKey:     "github_new_gist",
				Name:          "Any new Gist",
				Description:   "Triggers when there is a new Gist.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"description": "string",
					"url":         "string",
					"created_at":  "string",
					"owner":       "string",
				},
			},
			{
				UniqueKey:     "github_new_issue",
				Name:          "Any new issue",
				Description:   "Triggers when there is a new issue in a repository.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"repository": "string",
				},
				OutputSchema: map[string]interface{}{
					"issue_title":    "string",
					"issue_body":     "string",
					"issue_url":      "string",
					"user":           "string",
					"created_at":     "string",
					"repository_url": "string",
				},
			},
			{
				UniqueKey:     "github_new_closed_issue",
				Name:          "Any new closed issue",
				Description:   "Triggers when an issue is closed in a repository.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"repository": "string",
				},
				OutputSchema: map[string]interface{}{
					"issue_title":    "string",
					"issue_body":     "string",
					"issue_url":      "string",
					"user":           "string",
					"closed_at":      "string",
					"repository_url": "string",
				},
			},
			{
				UniqueKey:     "github_new_issue_assigned_to_you",
				Name:          "New issue assigned to you",
				Description:   "Triggers when a new issue is assigned to you.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"issue_title":    "string",
					"issue_body":     "string",
					"issue_url":      "string",
					"user":           "string",
					"assigned_at":    "string",
					"repository_url": "string",
				},
			},
			{
				UniqueKey:     "github_new_repository_by_username_or_org",
				Name:          "New repository by a specific username or organization",
				Description:   "Triggers when a new repository is created by a specific username or organization.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"username_or_organization": "string",
				},
				OutputSchema: map[string]interface{}{
					"repository_name": "string",
					"description":     "string",
					"url":             "string",
					"owner":           "string",
					"created_at":      "string",
					"repository_url":  "string",
				},
			},
			{
				UniqueKey:     "github_new_pull_request",
				Name:          "New pull request for a specific repository",
				Description:   "Triggers when there is a new pull request for a specific repository.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"repository": "string",
				},
				OutputSchema: map[string]interface{}{
					"title":      "string",
					"body":       "string",
					"url":        "string",
					"user":       "string",
					"created_at": "string",
				},
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
