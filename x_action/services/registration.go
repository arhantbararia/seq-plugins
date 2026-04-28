package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"x_action/models"
)

// x action plugin will register following capabilities:
// - plugin-service-provider: X.com
//   Auth: OAuth 2.0,
//   function: Post a tweet
//   required_config_data: Tweet text

// - plugin-service-provider: X.com
//   Auth: OAuth 2.0,
//   function: Post a tweet with image
//   required_config_data: Tweet text, image

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

	outputSchema := map[string]interface{}{
		"result": "object", // X.com API v2 response containing tweet data (id, text, etc.)
	}

	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-x-action"
	} else {
		pluginID = host + ":" + port + "-x-action"
	}

	prefix := "/x/action"
	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "X Action",
		ContainerType:         "action",
		PluginProviderService: "X",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":  prefix + "/setup",
			"remove": prefix + "/remove",
			"health": prefix + "/health",
		},
		AuthTypes: []string{"OAUTH2"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "post_a_tweet",
				Name:          "Post a Tweet",
				Description:   "Posts a tweet to the user's timeline.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"tweet_text": "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "post_a_tweet_with_image",
				Name:          "Post a Tweet with Image",
				Description:   "Posts a tweet with an image to the user's timeline.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"tweet_text": "string",
					"image_url":  "string",
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
