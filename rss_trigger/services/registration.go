package services

// Capabilities that are requried to be registered are:

// - plugin-service-provider: RSS Feed
//   Auth: None
//   function: New feed item
//   output: EntryTitle,  EntryUrl, EntryAuthor, EntryContent, EntryImageUrl, EntryPublished, FeedTitle, FeedUrl
//   required_config_data: Feed URL

// - plugin-service-provider: RSS Feed
//   Auth: None
//   function: New feed item matches
//   output: EntryTitle,  EntryUrl, EntryAuthor, EntryContent, EntryImageUrl, EntryPublished, FeedTitle, FeedUrl
//   required_config_data: Feed URL, keyword or phrase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"rss_trigger/models"
)

type Registration struct{}

func NewRegistrationService() *Registration {
	return &Registration{}
}

func (r *Registration) Register() error {
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
		pluginID = pluginID + "-rss-trigger"
	} else {
		pluginID = host + ":" + port + "-rss-trigger"
	}

	prefix := "/rss/trigger"

	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "RSS Trigger",
		ContainerType:         "TRIGGER",
		PluginProviderService: "RSS Feed",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":  prefix + "/setup",
			"remove": prefix + "/remove",
			"health": prefix + "/health",
		},
		AuthTypes: []string{"None"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "rss_new_feed_item",
				Name:          "New feed item",
				Description:   "Triggers when there is a new feed item.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"feed_url": "string",
				},
				OutputSchema: map[string]interface{}{
					"entry_title":     "string",
					"entry_url":       "string",
					"entry_author":    "string",
					"entry_content":   "string",
					"entry_image_url": "string",
					"entry_published": "string",
					"feed_title":      "string",
					"feed_url":        "string",
				},
			},
			{
				UniqueKey:     "rss_new_feed_item_matches",
				Name:          "New feed item matches",
				Description:   "Triggers when there is a new feed item matches.",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"feed_url": "string",
					"keyword":  "string",
				},
				OutputSchema: map[string]interface{}{
					"entry_title":     "string",
					"entry_url":       "string",
					"entry_author":    "string",
					"entry_content":   "string",
					"entry_image_url": "string",
					"entry_published": "string",
					"feed_title":      "string",
					"feed_url":        "string",
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
