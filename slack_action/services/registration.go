package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"slack_action/models"
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
		port = "8086"
	}

	prefix := "/slack/action"

	reg := models.RegistrationRequest{
		ID:                    "slack-action-01",
		Name:                  "slack",
		ContainerType:         "action",
		PluginProviderService: "Slack",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":    prefix + "/setup",
			"remove":   prefix + "/remove",
			"validate": prefix + "/validate",
			"health":   prefix + "/health",
		},
		AuthTypes: []string{"oauth2"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "slack_post_message",
				Name:          "Post a Message to a Channel",
				Description:   "Posts a message to the specified Slack channel. Supports attachments and an optional link. Requires a Slack Bot Token with the chat:write scope.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"channel": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Slack channel name (e.g. #general) or channel ID (e.g. C0123456789)",
					},
					"message": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "The message text to post. Supports Slack mrkdwn formatting.",
					},
					"attachments": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Optional JSON array of Slack attachment objects for rich formatting.",
					},
					"link": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Optional URL to append to the message body.",
					},
				},
				OutputSchema: map[string]interface{}{
					"ok":      "boolean",
					"channel": "string",
					"ts":      "string",
					"message": "object",
				},
			},
		},
	}

	body, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("marshal registration payload: %w", err)
	}

	url := executorURL + "/register"
	log.Printf("[Registration] Registering with executor at %s", url)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("registration request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("executor returned status %d", resp.StatusCode)
	}

	log.Println("[Registration] Successfully registered with workflow_executor")
	return nil
}