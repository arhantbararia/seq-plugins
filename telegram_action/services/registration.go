package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"telegram_action/models"
)

// RegistrationService handles self-registration with the workflow_executor.
type RegistrationService struct{}

func NewRegistrationService() *RegistrationService {
	return &RegistrationService{}
}

// Register sends a POST /register to the workflow_executor.
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
		port = "8086" // Default port for telegram_action
	}

	outputSchema := map[string]interface{}{
		"result": "object", // The Telegram API response for the sent message/media.
	}

	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-telegram-action"
	}

	prefix := os.Getenv("PLUGIN_ROUTE_PREFIX")
	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "Telegram Action",
		ContainerType:         "action",
		PluginProviderService: "Telegram",
		PluginHost:            host,
		PluginPort:            port,
		Endpoints: map[string]string{
			"setup":  prefix + "/setup",
			"remove": prefix + "/remove",
			"health": prefix + "/health",
		},
		AuthTypes: []string{"BOT_TOKEN"},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "telegram_send_message",
				Name:          "Send message",
				Description:   "Sends a text message to a specified chat. Read https://arhantbararia.github.io/2026/04/12/telegram_bot_token-retrieval-process.html on how to get bot token and chat id ",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id": "string",
					"message": "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "telegram_send_photo",
				Name:          "Send photo",
				Description:   "Sends a photo to a specified chat. Read https://arhantbararia.github.io/2026/04/12/telegram_bot_token-retrieval-process.html on how to get bot token and chat id",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id":  "string",
					"file_url": "string",
					"caption":  "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "telegram_send_video",
				Name:          "Send video",
				Description:   "Sends a video to a specified chat. Read https://arhantbararia.github.io/2026/04/12/telegram_bot_token-retrieval-process.html on how to get bot token and chat id",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id":  "string",
					"file_url": "string",
					"caption":  "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "telegram_send_mp3",
				Name:          "Send mp3",
				Description:   "Sends an MP3 audio file to a specified chat. Read https://arhantbararia.github.io/2026/04/12/telegram_bot_token-retrieval-process.html on how to get bot token and chat id",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"chat_id":  "string",
					"file_url": "string",
					"caption":  "string",
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
