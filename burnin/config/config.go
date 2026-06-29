package config

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const ConfigVersion = "2"

// AllPatternNames lists all 5 CE messaging patterns.
var AllPatternNames = []string{"events", "events_store", "queues", "commands", "queries"}

func IsRPCPattern(p string) bool {
	return p == "commands" || p == "queries"
}

type BrokerConfig struct {
	Address        string `yaml:"address" json:"address"`
	ClientIDPrefix string `yaml:"client_id_prefix" json:"client_id_prefix"`
}

type CEConfig struct {
	ContentMode string `yaml:"content_mode" json:"content_mode"`
	BaseURL     string `yaml:"base_url" json:"base_url"`
}

type PatternConfig struct {
	Enabled              bool `yaml:"enabled" json:"enabled"`
	Channels             int  `yaml:"channels" json:"channels"`
	ProducersPerChannel  int  `yaml:"producers_per_channel" json:"producers_per_channel"`
	ConsumersPerChannel  int  `yaml:"consumers_per_channel" json:"consumers_per_channel"`
	SendersPerChannel    int  `yaml:"senders_per_channel" json:"senders_per_channel"`
	RespondersPerChannel int  `yaml:"responders_per_channel" json:"responders_per_channel"`
	ConsumerGroup        bool `yaml:"consumer_group" json:"consumer_group"`
	Rate                 int  `yaml:"rate" json:"rate"`
}

type PatternsConfig struct {
	Events      PatternConfig `yaml:"events" json:"events"`
	EventsStore PatternConfig `yaml:"events_store" json:"events_store"`
	Queues      PatternConfig `yaml:"queues" json:"queues"`
	Commands    PatternConfig `yaml:"commands" json:"commands"`
	Queries     PatternConfig `yaml:"queries" json:"queries"`
}

type QueueConfig struct {
	PollMaxMessages        int `yaml:"poll_max_messages" json:"poll_max_messages"`
	PollWaitTimeoutSeconds int `yaml:"poll_wait_timeout_seconds" json:"poll_wait_timeout_seconds"`
	MaxDepth               int `yaml:"max_depth" json:"max_depth"`
}

type RPCConfig struct {
	TimeoutMs int `yaml:"timeout_ms" json:"timeout_ms"`
}

type MessageConfig struct {
	SizeMode         string `yaml:"size_mode" json:"size_mode"`
	SizeBytes        int    `yaml:"size_bytes" json:"size_bytes"`
	SizeDistribution string `yaml:"size_distribution" json:"size_distribution"`
	ReorderWindow    int    `yaml:"reorder_window" json:"reorder_window"`
}

type MetricsConfig struct {
	Port           int    `yaml:"port" json:"port"`
	ReportInterval string `yaml:"report_interval" json:"report_interval"`
}

type LoggingConfig struct {
	Format string `yaml:"format" json:"format"`
	Level  string `yaml:"level" json:"level"`
}

type ForcedDisconnConfig struct {
	Interval string `yaml:"interval" json:"interval"`
	Duration string `yaml:"duration" json:"duration"`
}

type RecoveryConfig struct {
	ReconnectInterval    string  `yaml:"reconnect_interval" json:"reconnect_interval"`
	ReconnectMaxInterval string  `yaml:"reconnect_max_interval" json:"reconnect_max_interval"`
	ReconnectMultiplier  float64 `yaml:"reconnect_multiplier" json:"reconnect_multiplier"`
}

type ShutdownConfig struct {
	DrainTimeoutSeconds int `yaml:"drain_timeout_seconds" json:"drain_timeout_seconds"`
}

type OutputConfig struct {
	ReportFile string `yaml:"report_file" json:"report_file"`
	SDKVersion string `yaml:"sdk_version" json:"sdk_version"`
}

type ThresholdsConfig struct {
	MaxLossPct            float64 `yaml:"max_loss_pct" json:"max_loss_pct"`
	MaxEventsLossPct      float64 `yaml:"max_events_loss_pct" json:"max_events_loss_pct"`
	MaxDuplicationPct     float64 `yaml:"max_duplication_pct" json:"max_duplication_pct"`
	MaxP99LatencyMS       float64 `yaml:"max_p99_latency_ms" json:"max_p99_latency_ms"`
	MaxP999LatencyMS      float64 `yaml:"max_p999_latency_ms" json:"max_p999_latency_ms"`
	MinThroughputPct      float64 `yaml:"min_throughput_pct" json:"min_throughput_pct"`
	MaxErrorRatePct       float64 `yaml:"max_error_rate_pct" json:"max_error_rate_pct"`
	MaxMemoryGrowthFactor float64 `yaml:"max_memory_growth_factor" json:"max_memory_growth_factor"`
	MaxDowntimePct        float64 `yaml:"max_downtime_pct" json:"max_downtime_pct"`
	MaxCEFidelityFailures int     `yaml:"max_ce_fidelity_failures" json:"max_ce_fidelity_failures"`
	MaxDuration           string  `yaml:"max_duration" json:"max_duration"`
}

type WarmupConfig struct {
	MaxParallelChannels int `yaml:"max_parallel_channels" json:"max_parallel_channels"`
	TimeoutPerChannelMs int `yaml:"timeout_per_channel_ms" json:"timeout_per_channel_ms"`
}

type CORSConfig struct {
	Origins string `yaml:"origins" json:"origins"`
}

type Config struct {
	Version          string              `yaml:"version" json:"version"`
	Broker           BrokerConfig        `yaml:"broker" json:"broker"`
	Mode             string              `yaml:"mode" json:"mode"`
	Duration         string              `yaml:"duration" json:"duration"`
	RunID            string              `yaml:"run_id" json:"run_id"`
	WarmupDuration   string              `yaml:"warmup_duration" json:"warmup_duration"`
	CE               CEConfig            `yaml:"ce" json:"ce"`
	Patterns         PatternsConfig      `yaml:"patterns" json:"patterns"`
	Queue            QueueConfig         `yaml:"queue" json:"queue"`
	RPC              RPCConfig           `yaml:"rpc" json:"rpc"`
	Message          MessageConfig       `yaml:"message" json:"message"`
	Metrics          MetricsConfig       `yaml:"metrics" json:"metrics"`
	Logging          LoggingConfig       `yaml:"logging" json:"logging"`
	ForcedDisconnect ForcedDisconnConfig `yaml:"forced_disconnect" json:"forced_disconnect"`
	Recovery         RecoveryConfig      `yaml:"recovery" json:"recovery"`
	Shutdown         ShutdownConfig      `yaml:"shutdown" json:"shutdown"`
	Output           OutputConfig        `yaml:"output" json:"output"`
	Thresholds       ThresholdsConfig    `yaml:"thresholds" json:"thresholds"`
	Warmup           WarmupConfig        `yaml:"warmup" json:"warmup"`
	CORS             CORSConfig          `yaml:"cors" json:"cors"`

	DurationParsed        time.Duration `yaml:"-" json:"-"`
	WarmupDurationParsed  time.Duration `yaml:"-" json:"-"`
	ReportIntervalParsed  time.Duration `yaml:"-" json:"-"`
	ForcedDisconnInterval time.Duration `yaml:"-" json:"-"`
	ForcedDisconnDuration time.Duration `yaml:"-" json:"-"`
	ReconnectInterval     time.Duration `yaml:"-" json:"-"`
	ReconnectMaxInterval  time.Duration `yaml:"-" json:"-"`
	MaxDurationParsed     time.Duration `yaml:"-" json:"-"`
	Warnings              []string      `yaml:"-" json:"-"`
}

func DefaultConfig() *Config {
	c := &Config{}
	c.Version = ConfigVersion
	c.Broker.Address = "localhost:9090"
	c.Broker.ClientIDPrefix = "burnin-ce"
	c.Mode = "soak"
	c.Duration = "1h"
	c.CE.ContentMode = "structured"

	c.Patterns = PatternsConfig{
		Events: PatternConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 100,
		},
		EventsStore: PatternConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 100,
		},
		Queues: PatternConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		Commands: PatternConfig{
			Enabled: true, Channels: 1,
			SendersPerChannel: 1, RespondersPerChannel: 1,
			Rate: 20,
		},
		Queries: PatternConfig{
			Enabled: true, Channels: 1,
			SendersPerChannel: 1, RespondersPerChannel: 1,
			Rate: 20,
		},
	}

	c.Queue = QueueConfig{
		PollMaxMessages:        10,
		PollWaitTimeoutSeconds: 5,
		MaxDepth:               1_000_000,
	}
	c.RPC.TimeoutMs = 5000

	c.Message = MessageConfig{
		SizeMode:         "fixed",
		SizeBytes:        1024,
		SizeDistribution: "256:80,4096:15,65536:5",
		ReorderWindow:    10_000,
	}

	c.Metrics = MetricsConfig{
		Port:           8895,
		ReportInterval: "30s",
	}

	c.Logging = LoggingConfig{Format: "text", Level: "info"}

	c.ForcedDisconnect = ForcedDisconnConfig{
		Interval: "0",
		Duration: "5s",
	}

	c.Recovery = RecoveryConfig{
		ReconnectInterval:    "1s",
		ReconnectMaxInterval: "30s",
		ReconnectMultiplier:  2.0,
	}

	c.Shutdown.DrainTimeoutSeconds = 10

	c.Thresholds = ThresholdsConfig{
		MaxLossPct:            0.0,
		MaxEventsLossPct:      5.0,
		MaxDuplicationPct:     0.1,
		MaxP99LatencyMS:       2000,
		MaxP999LatencyMS:      10000,
		MinThroughputPct:      80,
		MaxErrorRatePct:       1.0,
		MaxMemoryGrowthFactor: 2.0,
		MaxDowntimePct:        10,
		MaxCEFidelityFailures: 0,
		MaxDuration:           "168h",
	}

	c.Warmup = WarmupConfig{
		MaxParallelChannels: 10,
		TimeoutPerChannelMs: 5000,
	}

	c.CORS.Origins = "*"

	return c
}

func Load(path string) (*Config, error) {
	c := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", path, err)
		}

		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(c); err != nil {
			c.Warnings = append(c.Warnings, fmt.Sprintf("config has unknown fields: %v", err))
			c2 := DefaultConfig()
			if err2 := yaml.Unmarshal(data, c2); err2 != nil {
				return nil, fmt.Errorf("parse config file %s: %w", path, err2)
			}
			*c = *c2
			c.Warnings = append(c.Warnings, fmt.Sprintf("config has unknown fields: %v", err))
		}
	}

	applyEnvOverrides(c)

	if c.CE.BaseURL == "" {
		c.CE.BaseURL = "http://" + c.Broker.Address
	}

	if err := parseDurations(c); err != nil {
		return nil, err
	}

	if c.RunID == "" {
		c.RunID = RandomRunID()
	}

	return c, nil
}

func FindConfigFile(cliPath string) string {
	if cliPath != "" {
		return cliPath
	}
	if _, err := os.Stat("./burnin-config.yaml"); err == nil {
		return "./burnin-config.yaml"
	}
	if _, err := os.Stat("/etc/burnin/config.yaml"); err == nil {
		return "/etc/burnin/config.yaml"
	}
	return ""
}

func (c *Config) GetPatternConfig(pattern string) *PatternConfig {
	switch pattern {
	case "events":
		return &c.Patterns.Events
	case "events_store":
		return &c.Patterns.EventsStore
	case "queues":
		return &c.Patterns.Queues
	case "commands":
		return &c.Patterns.Commands
	case "queries":
		return &c.Patterns.Queries
	default:
		return nil
	}
}

func (c *Config) GetPatternRate(pattern string) int {
	if pc := c.GetPatternConfig(pattern); pc != nil {
		return pc.Rate
	}
	return 100
}

func (c *Config) GetPatternChannels(pattern string) int {
	if pc := c.GetPatternConfig(pattern); pc != nil {
		return pc.Channels
	}
	return 1
}

func (c *Config) TotalChannelCount() int {
	total := 0
	for _, pname := range AllPatternNames {
		if pc := c.GetPatternConfig(pname); pc != nil && pc.Enabled {
			total += pc.Channels
		}
	}
	return total
}

func (c *Config) Validate() []error {
	var errs []error

	if c.Version != ConfigVersion {
		errs = append(errs, fmt.Errorf("version must be %q, got %q", ConfigVersion, c.Version))
	}

	if c.Broker.Address == "" {
		errs = append(errs, fmt.Errorf("broker.address is required"))
	}

	validContentModes := map[string]bool{"structured": true, "binary": true, "alternating": true}
	if !validContentModes[c.CE.ContentMode] {
		errs = append(errs, fmt.Errorf("ce.content_mode must be 'structured', 'binary', or 'alternating', got %q", c.CE.ContentMode))
	}

	enabledCount := 0
	totalWorkers := 0
	totalSSEConsumers := 0

	for _, pname := range AllPatternNames {
		pc := c.GetPatternConfig(pname)
		if pc == nil || !pc.Enabled {
			continue
		}
		enabledCount++

		if pc.Channels < 1 || pc.Channels > 1000 {
			errs = append(errs, fmt.Errorf("%s.channels: must be 1-1000, got %d", pname, pc.Channels))
		}

		if pc.Rate <= 0 {
			errs = append(errs, fmt.Errorf("%s.rate: must be > 0, got %d", pname, pc.Rate))
		}

		if IsRPCPattern(pname) {
			if pc.SendersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.senders_per_channel: must be >= 1, got %d", pname, pc.SendersPerChannel))
			}
			if pc.RespondersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.responders_per_channel: must be >= 1, got %d", pname, pc.RespondersPerChannel))
			}
			totalWorkers += pc.Channels * (pc.SendersPerChannel + pc.RespondersPerChannel)
			totalSSEConsumers += pc.Channels * pc.RespondersPerChannel
		} else {
			if pc.ProducersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.producers_per_channel: must be >= 1, got %d", pname, pc.ProducersPerChannel))
			}
			if pc.ConsumersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.consumers_per_channel: must be >= 1, got %d", pname, pc.ConsumersPerChannel))
			}
			totalWorkers += pc.Channels * (pc.ProducersPerChannel + pc.ConsumersPerChannel)
			if pname != "queues" {
				totalSSEConsumers += pc.Channels * pc.ConsumersPerChannel
			}
		}
	}

	if enabledCount == 0 {
		errs = append(errs, fmt.Errorf("at least one pattern must be enabled"))
	}

	if c.Message.SizeMode != "fixed" && c.Message.SizeMode != "distribution" {
		errs = append(errs, fmt.Errorf("message.size_mode must be 'fixed' or 'distribution', got %q", c.Message.SizeMode))
	}
	if c.Message.SizeMode == "fixed" && c.Message.SizeBytes < 64 {
		errs = append(errs, fmt.Errorf("message.size_bytes: must be >= 64, got %d", c.Message.SizeBytes))
	}
	if c.Message.ReorderWindow < 100 {
		errs = append(errs, fmt.Errorf("message.reorder_window: must be >= 100, got %d", c.Message.ReorderWindow))
	}

	if c.Queue.PollMaxMessages < 1 || c.Queue.PollMaxMessages > 1000 {
		errs = append(errs, fmt.Errorf("queue.poll_max_messages: must be 1-1000, got %d", c.Queue.PollMaxMessages))
	}
	if c.Queue.PollWaitTimeoutSeconds <= 0 {
		errs = append(errs, fmt.Errorf("queue.poll_wait_timeout_seconds: must be > 0, got %d", c.Queue.PollWaitTimeoutSeconds))
	}

	if c.RPC.TimeoutMs <= 0 {
		errs = append(errs, fmt.Errorf("rpc.timeout_ms: must be > 0, got %d", c.RPC.TimeoutMs))
	}

	if c.Shutdown.DrainTimeoutSeconds <= 0 {
		errs = append(errs, fmt.Errorf("shutdown.drain_timeout_seconds: must be > 0, got %d", c.Shutdown.DrainTimeoutSeconds))
	}

	if c.Metrics.Port < 1 || c.Metrics.Port > 65535 {
		errs = append(errs, fmt.Errorf("metrics.port: must be 1-65535, got %d", c.Metrics.Port))
	}

	if c.Thresholds.MaxLossPct < 0 || c.Thresholds.MaxLossPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_loss_pct: must be 0-100"))
	}
	if c.Thresholds.MaxEventsLossPct < 0 || c.Thresholds.MaxEventsLossPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_events_loss_pct: must be 0-100"))
	}
	if c.Thresholds.MaxDuplicationPct < 0 || c.Thresholds.MaxDuplicationPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_duplication_pct: must be 0-100"))
	}
	if c.Thresholds.MaxP99LatencyMS <= 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_p99_latency_ms: must be > 0"))
	}
	if c.Thresholds.MaxP999LatencyMS <= 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_p999_latency_ms: must be > 0"))
	}
	if c.Thresholds.MinThroughputPct <= 0 || c.Thresholds.MinThroughputPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.min_throughput_pct: must be > 0 and <= 100"))
	}
	if c.Thresholds.MaxErrorRatePct < 0 || c.Thresholds.MaxErrorRatePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_error_rate_pct: must be 0-100"))
	}
	if c.Thresholds.MaxMemoryGrowthFactor < 1.0 {
		errs = append(errs, fmt.Errorf("thresholds.max_memory_growth_factor: must be >= 1.0"))
	}
	if c.Thresholds.MaxDowntimePct < 0 || c.Thresholds.MaxDowntimePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_downtime_pct: must be 0-100"))
	}
	if c.Thresholds.MaxCEFidelityFailures < 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_ce_fidelity_failures: must be >= 0"))
	}
	if c.Recovery.ReconnectMultiplier < 1.0 {
		errs = append(errs, fmt.Errorf("recovery.reconnect_multiplier: must be >= 1.0, got %f", c.Recovery.ReconnectMultiplier))
	}

	if totalWorkers > 500 {
		errs = append(errs, fmt.Errorf("WARNING: high worker count: %d -- may impact system resources", totalWorkers))
	}
	estMemoryMB := float64(totalWorkers) * (float64(c.Message.ReorderWindow)*8/1024/1024 + 0.5)
	if estMemoryMB > 4096 {
		errs = append(errs, fmt.Errorf("WARNING: estimated memory %.0f MB exceeds 4096 MB for %d workers", estMemoryMB, totalWorkers))
	}
	if totalSSEConsumers > 50 {
		errs = append(errs, fmt.Errorf("WARNING: %d SSE consumers -- verify KubeMQ server MaxSSEConnections is set high enough", totalSSEConsumers))
	}

	return errs
}

func (c *Config) LogResourceWarnings(logger *slog.Logger) {
	errs := c.Validate()
	for _, e := range errs {
		if strings.HasPrefix(e.Error(), "WARNING:") {
			logger.Warn(e.Error())
		}
	}
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("KUBEMQ_BROKER_ADDRESS"); v != "" {
		cfg.Broker.Address = v
	}
	if v := os.Getenv("CE_CONTENT_MODE"); v != "" {
		cfg.CE.ContentMode = v
	}
	if v := os.Getenv("CE_BASE_URL"); v != "" {
		cfg.CE.BaseURL = v
	}

	type patternEnv struct {
		name    string
		envName string
	}
	patterns := []patternEnv{
		{"events", "EVENTS"},
		{"events_store", "EVENTS_STORE"},
		{"queues", "QUEUES"},
		{"commands", "COMMANDS"},
		{"queries", "QUERIES"},
	}

	for _, p := range patterns {
		pc := cfg.GetPatternConfig(p.name)
		if pc == nil {
			continue
		}
		if v := os.Getenv("BURNIN_" + p.envName + "_RATE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				pc.Rate = n
			}
		}
		if v := os.Getenv("BURNIN_" + p.envName + "_CHANNELS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				pc.Channels = n
			}
		}
		if IsRPCPattern(p.name) {
			if v := os.Getenv("BURNIN_" + p.envName + "_SENDERS"); v != "" {
				if n, err := strconv.Atoi(v); err == nil {
					pc.SendersPerChannel = n
				}
			}
			if v := os.Getenv("BURNIN_" + p.envName + "_RESPONDERS"); v != "" {
				if n, err := strconv.Atoi(v); err == nil {
					pc.RespondersPerChannel = n
				}
			}
		} else {
			if v := os.Getenv("BURNIN_" + p.envName + "_PRODUCERS"); v != "" {
				if n, err := strconv.Atoi(v); err == nil {
					pc.ProducersPerChannel = n
				}
			}
			if v := os.Getenv("BURNIN_" + p.envName + "_CONSUMERS"); v != "" {
				if n, err := strconv.Atoi(v); err == nil {
					pc.ConsumersPerChannel = n
				}
			}
		}
	}
}

func parseDurations(c *Config) error {
	var err error

	if c.Duration != "" && c.Duration != "0" {
		c.DurationParsed, err = parseDuration(c.Duration)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", c.Duration, err)
		}
	}

	if c.WarmupDuration != "" {
		c.WarmupDurationParsed, err = parseDuration(c.WarmupDuration)
		if err != nil {
			return fmt.Errorf("invalid warmup_duration %q: %w", c.WarmupDuration, err)
		}
	}

	if c.Metrics.ReportInterval != "" {
		c.ReportIntervalParsed, err = parseDuration(c.Metrics.ReportInterval)
		if err != nil {
			return fmt.Errorf("invalid metrics.report_interval %q: %w", c.Metrics.ReportInterval, err)
		}
	}

	if c.ForcedDisconnect.Interval != "" && c.ForcedDisconnect.Interval != "0" {
		c.ForcedDisconnInterval, err = parseDuration(c.ForcedDisconnect.Interval)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.interval %q: %w", c.ForcedDisconnect.Interval, err)
		}
	}

	if c.ForcedDisconnect.Duration != "" {
		c.ForcedDisconnDuration, err = parseDuration(c.ForcedDisconnect.Duration)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.duration %q: %w", c.ForcedDisconnect.Duration, err)
		}
	}

	if c.Recovery.ReconnectInterval != "" {
		c.ReconnectInterval, err = parseDuration(c.Recovery.ReconnectInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_interval %q: %w", c.Recovery.ReconnectInterval, err)
		}
	}

	if c.Recovery.ReconnectMaxInterval != "" {
		c.ReconnectMaxInterval, err = parseDuration(c.Recovery.ReconnectMaxInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_max_interval %q: %w", c.Recovery.ReconnectMaxInterval, err)
		}
	}

	if c.Thresholds.MaxDuration != "" {
		c.MaxDurationParsed, err = parseDuration(c.Thresholds.MaxDuration)
		if err != nil {
			return fmt.Errorf("invalid thresholds.max_duration %q: %w", c.Thresholds.MaxDuration, err)
		}
	}

	return nil
}

func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

func RandomRunID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%08x", b)
}

func ParseDurationsPublic(cfg *Config) error {
	return parseDurations(cfg)
}
