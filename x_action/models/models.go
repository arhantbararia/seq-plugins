package models

import "time"

// ActionConfig holds all configuration for a single action instance.
type ActionConfig struct {
	CapabilityKey string              `json:"capability_key"`
	AuthContext   map[string]AuthData `json:"_auth_context"`

	// RawConfig stores the full config map from /setup for template resolution.
	// Values may contain {{trigger.payload.X}} templates that get resolved at runtime.
	RawConfig map[string]interface{} `json:"raw_config,omitempty"`

	// X (Twitter) specific fields
	TweetText string `json:"tweet_text,omitempty"` // Text content for the tweet
	ImageURL  string `json:"image_url,omitempty"`  // URL of image to attach (for post_a_tweet_with_image)
}

// X uses OAuth 2.0
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
	ContainerType         string             `json:"container_type"`          //Action
	PluginProviderService string             `json:"plugin_provider_service"` // X
	PluginHost            string             `json:"plugin_host"`
	PluginPort            string             `json:"plugin_port"`
	Endpoints             map[string]string  `json:"endpoints"`
	AuthTypes             []string           `json:"auth_types"` //["OAUTH2"]
	Capabilities          []PluginCapability `json:"capabilities"`
}

// PluginCapability describes one triggering capability.
type PluginCapability struct {
	UniqueKey     string                 `json:"unique_key"`     // post_a_tweet
	Name          string                 `json:"name"`           // Post a Tweet
	Description   string                 `json:"description"`    // Posts a tweet to the user's timeline.
	ComponentType string                 `json:"component_type"` //ACTION
	ConfigSchema  map[string]interface{} `json:"config_schema"`  // {"tweet_text": "string"}
	OutputSchema  map[string]interface{} `json:"output_schema"`  // {"tweet_id": "string", "tweet_text": "string"}
}
