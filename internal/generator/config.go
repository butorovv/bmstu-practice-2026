package generator

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Mode string

const (
	ModeNormal        Mode = "normal"
	ModeHighHeartRate Mode = "high-heart-rate"
	ModeMixed         Mode = "mixed"
	ModeDuplicate     Mode = "duplicate"
	ModeRampUp        Mode = "ramp-up"
	ModeSpike         Mode = "spike"
	ModeSoak          Mode = "soak"
	ModeChaosReady    Mode = "chaos-ready"
)

const (
	DefaultTargetURL     = "http://localhost:8080/api/v1/telemetry"
	DefaultDevices       = 1000
	DefaultPatients      = 1000
	DefaultDuration      = 10 * time.Minute
	DefaultBatchSize     = 1
	DefaultMaxBatchSize  = 10
	DefaultBaseInterval  = 5 * time.Second
	DefaultRPSLimit      = 5000.0
	DefaultOutput        = "results/run-001.json"
	DefaultHTTPTimeout   = 10 * time.Second
	DefaultDrainTimeout  = 30 * time.Second
	DefaultRetryBackoff  = 500 * time.Millisecond
	DefaultMaxBackoff    = 30 * time.Second
	DefaultSpikeFactor   = 5
	maxContractBatchSize = 10
)

type Config struct {
	TargetURL    string
	Mode         Mode
	Devices      int
	Patients     int
	Duration     time.Duration
	BatchSize    int
	MaxBatchSize int
	BaseInterval time.Duration
	RPSLimit     float64
	Output       string
	HTTPTimeout  time.Duration
	DrainTimeout time.Duration
	RetryBackoff time.Duration
	MaxBackoff   time.Duration
	SpikeFactor  int
}

func LoadConfig(args []string) (Config, error) {
	cfg := Config{}
	flags := flag.NewFlagSet("generator", flag.ContinueOnError)

	flags.StringVar(&cfg.TargetURL, "target-url", envString("GENERATOR_TARGET_URL", DefaultTargetURL), "Ingestion telemetry endpoint")
	mode := flags.String("mode", envString("GENERATOR_MODE", string(ModeNormal)), "generation mode")
	flags.IntVar(&cfg.Devices, "devices", envInt("GENERATOR_DEVICES", DefaultDevices), "number of base devices")
	flags.IntVar(&cfg.Patients, "patients", envInt("GENERATOR_PATIENTS", DefaultPatients), "number of patients")
	flags.DurationVar(&cfg.Duration, "duration", envDuration("GENERATOR_DURATION", DefaultDuration), "generation duration")
	flags.IntVar(&cfg.BatchSize, "batch-size", envInt("GENERATOR_BATCH_SIZE", DefaultBatchSize), "normal batch size")
	flags.IntVar(&cfg.MaxBatchSize, "max-batch-size", envInt("GENERATOR_MAX_BATCH_SIZE", DefaultMaxBatchSize), "maximum recovery batch size")
	flags.DurationVar(&cfg.BaseInterval, "base-interval", envDuration("GENERATOR_BASE_INTERVAL", DefaultBaseInterval), "measurement interval per device")
	flags.Float64Var(&cfg.RPSLimit, "rps-limit", envFloat("GENERATOR_RPS_LIMIT", DefaultRPSLimit), "global HTTP request limit")
	flags.StringVar(&cfg.Output, "output", envString("GENERATOR_OUTPUT", DefaultOutput), "result JSON path")
	flags.DurationVar(&cfg.HTTPTimeout, "http-timeout", envDuration("GENERATOR_HTTP_TIMEOUT", DefaultHTTPTimeout), "timeout per HTTP attempt")
	flags.DurationVar(&cfg.DrainTimeout, "drain-timeout", envDuration("GENERATOR_DRAIN_TIMEOUT", DefaultDrainTimeout), "time to flush local buffers after generation")
	flags.DurationVar(&cfg.RetryBackoff, "retry-backoff", envDuration("GENERATOR_RETRY_BACKOFF", DefaultRetryBackoff), "initial retry backoff")
	flags.DurationVar(&cfg.MaxBackoff, "max-backoff", envDuration("GENERATOR_MAX_BACKOFF", DefaultMaxBackoff), "maximum retry backoff")
	flags.IntVar(&cfg.SpikeFactor, "spike-factor", envInt("GENERATOR_SPIKE_FACTOR", DefaultSpikeFactor), "spike device multiplier")

	if err := flags.Parse(args); err != nil {
		return Config{}, err
	}
	cfg.Mode = Mode(*mode)

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	parsedURL, err := url.ParseRequestURI(c.TargetURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return fmt.Errorf("target-url must be an absolute HTTP URL")
	}
	if !c.Mode.Valid() {
		return fmt.Errorf("unsupported mode %q", c.Mode)
	}
	if c.Devices <= 0 {
		return fmt.Errorf("devices must be positive")
	}
	if c.Patients <= 0 {
		return fmt.Errorf("patients must be positive")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	if c.BatchSize < 1 || c.BatchSize > maxContractBatchSize {
		return fmt.Errorf("batch-size must be between 1 and %d", maxContractBatchSize)
	}
	if c.MaxBatchSize < c.BatchSize || c.MaxBatchSize > maxContractBatchSize {
		return fmt.Errorf("max-batch-size must be between batch-size and %d", maxContractBatchSize)
	}
	if c.BaseInterval <= 0 {
		return fmt.Errorf("base-interval must be positive")
	}
	if c.RPSLimit <= 0 {
		return fmt.Errorf("rps-limit must be positive")
	}
	if strings.TrimSpace(c.Output) == "" {
		return fmt.Errorf("output must not be empty")
	}
	if c.HTTPTimeout <= 0 || c.DrainTimeout <= 0 {
		return fmt.Errorf("http-timeout and drain-timeout must be positive")
	}
	if c.RetryBackoff <= 0 || c.MaxBackoff < c.RetryBackoff {
		return fmt.Errorf("retry backoff values are invalid")
	}
	if c.SpikeFactor < 1 {
		return fmt.Errorf("spike-factor must be positive")
	}
	maxInt := int(^uint(0) >> 1)
	if c.Mode == ModeSpike && c.Devices > maxInt/c.SpikeFactor {
		return fmt.Errorf("devices multiplied by spike-factor is too large")
	}
	return nil
}

func (m Mode) Valid() bool {
	switch m {
	case ModeNormal,
		ModeHighHeartRate,
		ModeMixed,
		ModeDuplicate,
		ModeRampUp,
		ModeSpike,
		ModeSoak,
		ModeChaosReady:
		return true
	default:
		return false
	}
}

func envString(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	parsed, err := strconv.Atoi(value)
	if value == "" || err != nil {
		return fallback
	}
	return parsed
}

func envFloat(name string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(name))
	parsed, err := strconv.ParseFloat(value, 64)
	if value == "" || err != nil {
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	parsed, err := time.ParseDuration(value)
	if value == "" || err != nil {
		return fallback
	}
	return parsed
}
