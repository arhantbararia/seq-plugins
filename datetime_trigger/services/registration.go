package services

// plugin will register following capabilities to workflow_executor

// - plugin-service-provider: Date & Time
//   Auth: None
//   function: Every day at
//   output: CheckTime
//   required_config_data: Specific time

// - plugin-service-provider: Date & Time
//   Auth: None
//   function: Every hour at
//   output: CheckTime
//   required_config_data: Specific time

// - plugin-service-provider: Date & Time
//   Auth: None
//   function: Every day of the week at
//   output: CheckTime
//   required_config_data: Day of week, time

// - plugin-service-provider: Date & Time
//   Auth: None
//   function: Every month on the
//   output: CheckTime
//   required_config_data: Day of month, time

// - plugin-service-provider: Date & Time
//   Auth: None
//   function: Every year on
//   output: CheckTime
//   required_config_data: Date, time

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"datetime_trigger/models"
)

// RegistrationService handles self-registration with the workflow_executor.
type RegistrationService struct{}

func NewRegistrationService() *RegistrationService {
	return &RegistrationService{}
}

// Register sends a POST /register to the workflow_executor.
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
		port = "8085"
	}

	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-datetime-trigger"
	}

	prefix := "/datetime/trigger"
	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "Date & Time Trigger",
		ContainerType:         "trigger",
		PluginProviderService: "Datetime",
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
				UniqueKey:     "datetime_every_day_at",
				Name:          "Every day at",
				Description:   "Triggers once every day at a specific time (HH:MM UTC).",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"scheduled_at": "string", // "HH:MM"
				},
				OutputSchema: map[string]interface{}{
					"check_time": "string", // RFC3339 UTC timestamp when the trigger fired
				},
			},
			{
				UniqueKey:     "datetime_every_hour_at",
				Name:          "Every hour at",
				Description:   "Triggers once every hour at a specific minute (MM).",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"scheduled_at": "string", // "MM" (00-59)
				},
				OutputSchema: map[string]interface{}{
					"check_time": "string", // RFC3339 UTC timestamp when the trigger fired
				},
			},
			{
				UniqueKey:     "datetime_every_day_of_week_at",
				Name:          "Every day of the week at",
				Description:   "Triggers once per week on a specific day and time (UTC).",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"day_of_week":  "string", // e.g. "Monday"
					"scheduled_at": "string", // "HH:MM"
				},
				OutputSchema: map[string]interface{}{
					"check_time": "string", // RFC3339 UTC timestamp when the trigger fired
				},
			},
			{
				UniqueKey:     "datetime_every_month_on",
				Name:          "Every month on the",
				Description:   "Triggers once per month on a specific day-of-month and time (UTC).",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"day_of_month": "integer", // 1-31
					"scheduled_at": "string",  // "HH:MM"
				},
				OutputSchema: map[string]interface{}{
					"check_time": "string", // RFC3339 UTC timestamp when the trigger fired
				},
			},
			{
				UniqueKey:     "datetime_every_year_on",
				Name:          "Every year on",
				Description:   "Triggers once per year on a specific date and time (UTC).",
				ComponentType: "TRIGGER",
				ConfigSchema: map[string]interface{}{
					"scheduled_at": "string", // "MM-DD HH:MM"
				},
				OutputSchema: map[string]interface{}{
					"check_time": "string", // RFC3339 UTC timestamp when the trigger fired
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
