package services

//agent pls complete and go through for any errors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"youtube_trigger/models"
)

// RegistrationService handles self-registration with the workflow_executor.
type RegistrationService struct{}

func NewRegistrationService() *RegistrationService {
	return &RegistrationService{}
}

func (s *RegistrationService) Register() error {
	executorURL := os.Getenv("WORKFLOW_EXECUTOR_URL")
	if executorURL == "" {
		executorURL = "http://localhost:8082"
	}
	host := os.Getenv("PLUGIN_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PLUGIN_PORT")
	if port == "" {
		port = "8085"
	}

	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-youtube-trigger"
	}

	prefix := os.Getenv("PLUGIN_ROUTE_PREFIX")
	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "YouTube Trigger",
		ContainerType:         "trigger",
		PluginProviderService: "YouTube",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":  prefix + "/setup",
			"remove": prefix + "/remove",
			"health": prefix + "/health",
		},
		AuthTypes:             []string{"OAuth 2.0"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "youtube_new_video_from_search",
				Name:          "New video from search",
				Description:   "Triggers when there is a new video from search.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"search_query": "string",
				},
				OutputSchema: map[string]interface{}{
					"title":        "string",
					"description":  "string",
					"url":          "string",
					"author_name":  "string",
					"published_at": "string",
				},
			},
			{
				UniqueKey:     "youtube_new_liked_video",
				Name:          "New liked video",
				Description:   "Triggers when there is a new liked video.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"title":       "string",
					"description": "string",
					"url":         "string",
					"author_name": "string",
					"liked_at":    "string",
				},
			},
			{
				UniqueKey:     "youtube_subscribe_to_channel",
				Name:          "You subscribe to a channel",
				Description:   "Triggers when you subscribe to a channel.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"channel_name": "string",
					"channel_url":  "string",
					"description":  "string",
				},
			},
			{
				UniqueKey:     "youtube_new_video_by_channel",
				Name:          "New video by channel",
				Description:   "Triggers when there is a new video by a specific channel.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"channel_name_or_id": "string",
				},
				OutputSchema: map[string]interface{}{
					"title":        "string",
					"description":  "string",
					"url":          "string",
					"author_name":  "string",
					"published_at": "string",
				},
			},
			{
				UniqueKey:     "youtube_new_playlist",
				Name:          "New playlist",
				Description:   "Triggers when there is a new playlist.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"title":        "string",
					"description":  "string",
					"url":          "string",
					"published_at": "string",
				},
			},
			{
				UniqueKey:     "youtube_new_public_video_uploaded_by_you",
				Name:          "New public video uploaded by you",
				Description:   "Triggers when you upload a new public video.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"title":        "string",
					"description":  "string",
					"url":          "string",
					"embed_code":   "string",
					"published_at": "string",
				},
			},
			{
				UniqueKey:     "youtube_new_public_video_from_subscriptions",
				Name:          "New public video from subscriptions",
				Description:   "Triggers when there is a new public video from your subscriptions.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"title":        "string",
					"description":  "string",
					"url":          "string",
					"author_name":  "string",
					"published_at": "string",
				},
			},
			{
				UniqueKey:     "youtube_new_super_chat_message",
				Name:          "New Super Chat message",
				Description:   "Triggers when there is a new Super Chat message.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"message":      "string",
					"author_name":  "string",
					"amount":       "string",
					"currency":     "string",
					"published_at": "string",
				},
			},
			{
				UniqueKey:     "youtube_new_channel_membership",
				Name:          "New channel membership",
				Description:   "Triggers when there is a new channel membership.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"member_name": "string",
					"level":       "string",
					"joined_at":   "string",
				},
			},
			{
				UniqueKey:     "youtube_new_super_sticker",
				Name:          "New Super Sticker",
				Description:   "Triggers when there is a new Super Sticker.",
				ComponentType: "TRIGGER",
				ConfigSchema:  map[string]interface{}{},
				OutputSchema: map[string]interface{}{
					"sticker_url":  "string",
					"author_name":  "string",
					"amount":       "string",
					"currency":     "string",
					"published_at": "string",
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
