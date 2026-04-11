package models

import "time"

// TriggerConfig holds all configuration for a single trigger instance.
type TriggerConfig struct {
	CapabilityKey string              `json:"capability_key"`
	AuthContext   map[string]AuthData `json:"_auth_context"`

	// Cron / time-based fields
	ScheduledAt string `json:"scheduled_at,omitempty"` // "HH:MM" or "MM" (minutes) or "MM-DD HH:MM"
	DayOfWeek   string `json:"day_of_week,omitempty"`  // e.g. "Monday"
	DayOfMonth  int    `json:"day_of_month,omitempty"` // 1-31
}

// AuthData carries credentials (Date & Time trigger uses none, but kept for compatibility).
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

// TriggerEvent is the RabbitMQ message published when an event fires.
type TriggerEvent struct {
	ID            string                 `json:"id"`
	WorkflowID    string                 `json:"workflow_id"`
	TriggerID     string                 `json:"trigger_id"`
	Type          string                 `json:"type"`
	Name          string                 `json:"name"`
	CapabilityKey string                 `json:"capability_key"`
	Payload       map[string]interface{} `json:"payload"`
	Timestamp     time.Time              `json:"timestamp"`
}

// RegistrationRequest is sent to the workflow_executor at startup.
type RegistrationRequest struct {
	ID                    string             `json:"id"`
	Name                  string             `json:"name"`
	ContainerType         string             `json:"container_type"`          //Trigger
	PluginProviderService string             `json:"plugin_provider_service"` // Cron
	PluginHost            string             `json:"plugin_host"`
	PluginPort            string             `json:"plugin_port"`
	Endpoints             map[string]string  `json:"endpoints"`
	AuthTypes             []string           `json:"auth_types"` //[]
	Capabilities          []PluginCapability `json:"capabilities"`
}

// PluginCapability describes one triggering capability.
type PluginCapability struct {
	UniqueKey     string                 `json:"unique_key"`     //datetime_every_hour_at
	Name          string                 `json:"name"`           // Every Hour
	Description   string                 `json:"description"`    // Triggers once every hour at a specific minute (MM).
	ComponentType string                 `json:"component_type"` //TRIGGER
	ConfigSchema  map[string]interface{} `json:"config_schema"`  // {"scheduled_at": "string"} // "MM"
	OutputSchema  map[string]interface{} `json:"output_schema"`  // {"check_time": "string"} // RFC3339 UTC timestamp when the trigger fired
}
