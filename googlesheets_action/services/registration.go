package services

//agent pls complete and check for any errors and smooth operation
import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"googlesheets_action/models"
)

//plugin will register following capabilities to the workflow executor

// - plugin-service-provider: Google Sheets
//   Auth: OAuth 2.0
//   function: Update cell in spreadsheet
//   required_config_data: Spreadsheet ID, worksheet, cell coordinates, value

// - plugin-service-provider: Google Sheets
//   Auth: OAuth 2.0
//   function: Add row to spreadsheet
//   required_config_data: Spreadsheet ID, worksheet, row values

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
		port = "8085"
	}

	outputSchema := map[string]interface{}{
		"result": "object",
	}
	// Build a unique ID: HOSTNAME is shared across all plugins in a unified container,
	// so we append a plugin-specific suffix to ensure each gets its own DB row.
	pluginID := os.Getenv("HOSTNAME")
	if pluginID != "" {
		pluginID = pluginID + "-googlesheets-action"
	} else {
		pluginID = host + ":" + port + "-googlesheets-action"
	}
	prefix := "/googlesheets/action"
	req := models.RegistrationRequest{
		ID:                    pluginID,
		Name:                  "Google Sheets Action",
		ContainerType:         "action",
		PluginProviderService: "Googlesheets",
		PluginHost:            host,
		PluginPort:            port,
		AuthTypes:             []string{"OAUTH2"},
		Endpoints: map[string]string{
			"setup":    prefix + "/setup",
			"remove":   prefix + "/remove",
			"validate": prefix + "/validate",
			"health":   prefix + "/health",
		},
		Capabilities: []models.PluginCapability{
			{
				UniqueKey:     "googlesheets_update_cell",
				Name:          "Update cell in spreadsheet",
				Description:   "Updates a specific cell in a Google Sheet.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"spreadsheet_id":   "string",
					"worksheet":        "string",
					"cell_coordinates": "string",
					"value":            "string",
				},
				OutputSchema: outputSchema,
			},
			{
				UniqueKey:     "googlesheets_add_row",
				Name:          "Add row to spreadsheet",
				Description:   "Appends a row to a Google Sheet.",
				ComponentType: "ACTION",
				ConfigSchema: map[string]interface{}{
					"spreadsheet_id": "string",
					"worksheet":      "string",
					"row_values":     "string",
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
