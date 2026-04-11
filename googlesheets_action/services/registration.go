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

	outputSchema := map[string]interface{}{
		"result": "object", // Google Sheets API response
	}

	req := models.RegistrationRequest{
		ID:                    os.Getenv("HOSTNAME"), // Docker sets the container ID as the HOSTNAME env var
		Name:                  "Google Sheets Action",
		ContainerType:         "action",
		PluginProviderService: "Google Sheets",
		PluginPort:            port,
		AuthTypes:             []string{"OAUTH2"},
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
