package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"instagram_trigger/models"
)

// // this trigger will register following capabilities to workflow executor.
// - plugin-service-provider: Instagram
//   Auth: OAuth 2.0
//   function: Any new photo by you
//   output: Caption, Url, SourceUrl, CreatedAt
//   required_config_data: None

// - plugin-service-provider: Instagram
//   Auth: OAuth 2.0
//   function: New photo by you with specific hashtag
//   output: Caption, Url, SourceUrl, CreatedAt
//   required_config_data: Hashtag

// - plugin-service-provider: Instagram
//   Auth: OAuth 2.0
//   function: Any new video by you
//   output: Caption, Url, SourceUrl, CreatedAt
//   required_config_data: None

// - plugin-service-provider: Instagram
//   Auth: OAuth 2.0
//   function: New video by you with specific hashtag
//   output: Caption, Url, SourceUrl, CreatedAt
//   required_config_data: Hashtag

type RegistrationService struct {
}

func NewRegistrationService() *RegistrationService {

	return &RegistrationService{}
}

func (r *RegistrationService) Register() error {
	executorURL := os.Getenv("EXECUTOR_URL")
	if executorURL == "" {
		return fmt.Errorf("EXECUTOR_URL not set")
	}

	host := os.Getenv("PLUGIN_HOST")
	if host == "" {
		return fmt.Errorf("PLUGIN_HOST not set")
	}

	port := os.Getenv("PLUGIN_PORT")
	if port == "" {
		return fmt.Errorf("PLUGIN_PORT not set")
	}

	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-instagram-trigger"
	} else {
		pluginID = host + ":" + port + "-instagram-trigger"
	}

	prefix := "/instagram/trigger"

	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "Instagram Trigger",
		ContainerType:         "TRIGGER",
		PluginProviderService: "Instagram",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":    prefix + "/setup",
			"remove":   prefix + "/remove",
			"validate": prefix + "/validate",
			"health":   prefix + "/health",
		},
		AuthTypes: []string{"OAUTH"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "any_new_photo_by_you",
				Name:          "Any New Photo by you",
				Description:   "Triggers when a new photo is posted by you. Requires an Instagram Business or Creator Account.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"caption":    "string",
					"url":        "string",
					"source_url": "string",
					"created_at": "string",
				},
			},
			{
				UniqueKey:     "new_photo_by_you_with_hashtag",
				Name:          "New photo by you with specific hashtag",
				Description:   "Triggers when a new photo is posted by you with a specific hashtag. Requires an Instagram Business or Creator Account.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{"hashtag": "string"},
				OutputSchema: map[string]interface{}{
					"caption":    "string",
					"url":        "string",
					"source_url": "string",
					"created_at": "string",
				},
			},
			{
				UniqueKey:     "any_new_video_by_you",
				Name:          "Any New Video by you",
				Description:   "Triggers when a new video is posted by you. Requires an Instagram Business or Creator Account.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"caption":    "string",
					"url":        "string",
					"source_url": "string",
					"created_at": "string",
				},
			},
			{
				UniqueKey:     "new_video_by_you_with_hashtag",
				Name:          "New video by you with specific hashtag",
				Description:   "Triggers when a new video is posted by you with a specific hashtag. Requires an Instagram Business or Creator Account.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{"hashtag": "string"},
				OutputSchema: map[string]interface{}{
					"caption":    "string",
					"url":        "string",
					"source_url": "string",
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
