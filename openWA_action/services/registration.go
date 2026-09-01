package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"openwa_action/models"
	"os"
)

// RegisterPlugin sends the registration metadata to the workflow executor.
func RegisterPlugin() error {
	executorURL := os.Getenv("EXECUTOR_URL")
	if executorURL == "" {
		executorURL = "http://localhost:8082" // default executor url
	}

	host := os.Getenv("PLUGIN_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PLUGIN_PORT")
	if port == "" {
		port = "8088" // New port for OpenWA plugin
	}

	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-openwa-action"
	} else {
		pluginID = host + ":" + port + "-openwa-action"
	}

	prefix := "/openWA/action"
	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "OpenWA Action",
		ContainerType:         "action",
		PluginProviderService: "OpenWA",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":  prefix + "/setup",
			"remove": prefix + "/remove",
			"health": prefix + "/health",
		},
		AuthTypes: []string{"mobile_pairing"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "openwa_send_text",
				Name:          "Send text message",
				Description:   "Sends a WhatsApp text message via OpenWA. Uses the authenticated user's session, or falls back to the sequels-bot session. Supports {{trigger.payload.X}} templates.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated). e.g., '628123456789' for individuals, or 'groupId@g.us' for groups.",
					},
					"text": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Message text (max 4096 chars). Supports {{trigger.payload.X}} templates.",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_image",
				Name:          "Send image",
				Description:   "Sends an image by URL with optional caption.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"url": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Publicly accessible image URL.",
					},
					"caption": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Optional image caption.",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_video",
				Name:          "Send video",
				Description:   "Sends a video by URL with optional caption.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"url": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Publicly accessible video URL.",
					},
					"caption": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Optional video caption.",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_audio",
				Name:          "Send audio",
				Description:   "Sends an audio file or voice note by URL.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"url": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Publicly accessible audio URL.",
					},
					"ptt": map[string]interface{}{
						"type":        "boolean",
						"required":    false,
						"description": "If true, sends as a voice note (push-to-talk). Defaults to false.",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_document",
				Name:          "Send document",
				Description:   "Sends a document/file by URL.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"url": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Publicly accessible document URL.",
					},
					"filename": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Optional document display name.",
					},
					"caption": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Optional document caption.",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_location",
				Name:          "Send location",
				Description:   "Sends a location pin with coordinates.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"latitude": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Location latitude.",
					},
					"longitude": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Location longitude.",
					},
					"description": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "Optional location description.",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_contact",
				Name:          "Send contact card",
				Description:   "Sends a vCard contact to a chat.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"contact_name": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Contact display name.",
					},
					"contact_number": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Contact phone number.",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_template",
				Name:          "Send template message",
				Description:   "Sends a pre-defined OpenWA template with variable substitution.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"template_name": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Stored template name.",
					},
					"template_vars": map[string]interface{}{
						"type":        "string",
						"required":    false,
						"description": "JSON string of key-value pairs for variables (e.g. `{\"customer\": \"Alice\"}`).",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_sticker",
				Name:          "Send sticker",
				Description:   "Sends a WebP sticker by URL.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"url": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Publicly accessible sticker URL (must be WebP).",
					},
				},
				OutputSchema: map[string]interface{}{
					"message_id": "string",
					"timestamp":  "string",
				},
			},
			{
				UniqueKey:     "openwa_send_typing",
				Name:          "Show typing indicator",
				Description:   "Shows typing/recording status in a chat.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Recipient phone numbers (comma-separated).",
					},
					"state": map[string]interface{}{
						"type":        "string",
						"required":    true,
						"description": "Status state. Must be one of: 'typing', 'recording', 'paused'.",
					},
				},
				OutputSchema: map[string]interface{}{
					"success": "string",
				},
			},
		},
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal registration request: %v", err)
	}

	url := fmt.Sprintf("%s/register", executorURL)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to send registration request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("registration failed with status %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("Successfully registered plugin %s with %d capabilities", req.Name, len(req.Capabilities))
	return nil
}
