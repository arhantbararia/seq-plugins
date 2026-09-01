package models

import "time"

// ActionConfig holds all configuration for a single action instance.
type ActionConfig struct {
	CapabilityKey string              `json:"capability_key"`
	AuthContext   map[string]AuthData `json:"_auth_context"`

	// RawConfig stores the full config map from /setup for template resolution.
	// Values may contain {{trigger.payload.X}} templates that get resolved at runtime.
	RawConfig map[string]interface{} `json:"raw_config,omitempty"`

	// OpenWA-specific fields
	ChatID        string `json:"chat_id,omitempty"`        // phone@c.us or groupId@g.us (comma-separated for multi-send)
	Text          string `json:"text,omitempty"`           // text message body
	URL           string `json:"url,omitempty"`            // media URL (image/video/audio/doc/sticker)
	Caption       string `json:"caption,omitempty"`        // media caption
	Filename      string `json:"filename,omitempty"`       // document filename
	Mimetype      string `json:"mimetype,omitempty"`       // MIME type for media
	Latitude      string `json:"latitude,omitempty"`       // location latitude
	Longitude     string `json:"longitude,omitempty"`      // location longitude
	Description   string `json:"description,omitempty"`    // location description
	ContactName   string `json:"contact_name,omitempty"`   // vCard contact name
	ContactNumber string `json:"contact_number,omitempty"` // vCard contact number
	TemplateName  string `json:"template_name,omitempty"`  // OpenWA template name
	TemplateVars  string `json:"template_vars,omitempty"`  // JSON string for template vars
	State         string `json:"state,omitempty"`          // typing/recording/paused
	PTT           bool   `json:"ptt,omitempty"`            // push-to-talk (voice note)
}

// AuthData is the universal credential envelope.
// For WhatsApp: api_key holds the Cloud API access token,
// external_account_id holds the Phone Number ID.
type AuthData struct {
	AccessToken       string    `json:"access_token"`
	RefreshToken      string    `json:"refresh_token"`
	TokenType         string    `json:"token_type"`
	Expiry            time.Time `json:"expiry"`
	Provider          string    `json:"provider"`
	ExternalAccountID string    `json:"external_account_id"`
	APIKey            string    `json:"api_key,omitempty"`
	BotToken          string    `json:"bot_token,omitempty"`
	SecretKey         string    `json:"secret_key,omitempty"`
	Username          string    `json:"username,omitempty"`
	Password          string    `json:"password,omitempty"`
}

type ActionTask struct {
	ID            string                 `json:"id"`
	WorkflowID    string                 `json:"workflow_id"`
	TriggerID     string                 `json:"trigger_id"`
	Type          string                 `json:"type"`
	Name          string                 `json:"name"`
	CapabilityKey string                 `json:"capability_key"`
	Payload       map[string]interface{} `json:"payload"`
	Timestamp     time.Time              `json:"timestamp"`
}

// ActionResult is published after execution to allow the executor to track job outcomes.
type ActionResult struct {
	TaskID       string                 `json:"task_id"`
	WorkflowID   string                 `json:"workflow_id"`
	Success      bool                   `json:"success"`
	Status       string                 `json:"status"` // "success" | "error"
	Output       map[string]interface{} `json:"output"` // Service response data
	Error        string                 `json:"error,omitempty"`
	RetryCount   int                    `json:"retry_count,omitempty"`
	ResponseTime int64                  `json:"response_time_ms,omitempty"`
	Timestamp    time.Time              `json:"timestamp"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// RegistrationRequest is sent to the workflow_executor at startup.
type RegistrationRequest struct {
	ID                    string             `json:"id"`
	Name                  string             `json:"name"`
	ContainerType         string             `json:"container_type"`          // "action"
	PluginProviderService string             `json:"plugin_provider_service"` // "WhatsApp"
	PluginHost            string             `json:"plugin_host"`
	PluginPort            string             `json:"plugin_port"`
	Endpoints             map[string]string  `json:"endpoints"`
	AuthTypes             []string           `json:"auth_types"` // ["API_KEY"]
	Capabilities          []PluginCapability `json:"capabilities"`
}

// PluginCapability describes one action capability.
type PluginCapability struct {
	UniqueKey     string                 `json:"unique_key"`      // whatsapp_send_message
	Name          string                 `json:"name"`            // Send message
	Description   string                 `json:"description"`     // Sends a WhatsApp text message...
	ComponentType string                 `json:"component_type"`  // ACTION
	ConfigSchema  map[string]interface{} `json:"config_schema"`   // {"to_phone_number": "string", "message": "string"}
	OutputSchema  map[string]interface{} `json:"output_schema"`   // {"result": "object"}
}
