package models

import "time"

// ActionConfig holds all configuration for a single action instance.
type ActionConfig struct {
	CapabilityKey string              `json:"capability_key"`
	AuthContext   map[string]AuthData `json:"_auth_context"`

	// RawConfig stores the full config map from /setup for template resolution.
	// Values may contain {{trigger.payload.X}} templates that get resolved at runtime.
	RawConfig map[string]interface{} `json:"raw_config,omitempty"`

	// Slack-specific fields
	Channel     string `json:"channel"`     // Channel name or ID (e.g. "#general" or "C0123456789")
	Message     string `json:"message"`     // Plain-text or mrkdwn message body
	Attachments string `json:"attachments"` // Optional: JSON string of Slack attachments array
	Link        string `json:"link"`        // Optional: URL to append/embed in the message
}

// Slack uses OAuth 2.0 (Bot Token)
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
	Status       string                 `json:"status"` // "success" | "error" | "retrying"
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
	ContainerType         string             `json:"container_type"`          // action
	PluginProviderService string             `json:"plugin_provider_service"` // Slack
	PluginHost            string             `json:"plugin_host"`
	PluginPort            string             `json:"plugin_port"`
	Endpoints             map[string]string  `json:"endpoints"`
	AuthTypes             []string           `json:"auth_types"` // ["oauth2"]
	Capabilities          []PluginCapability `json:"capabilities"`
}

// PluginCapability describes one action capability.
type PluginCapability struct {
	UniqueKey     string                 `json:"unique_key"`
	Name          string                 `json:"name"`
	Description   string                 `json:"description"`
	ComponentType string                 `json:"component_type"` // ACTION
	ConfigSchema  map[string]interface{} `json:"config_schema"`
	OutputSchema  map[string]interface{} `json:"output_schema"`
}
