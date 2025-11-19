package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Env      string      `mapstructure:"env"`
	LogLevel string      `mapstructure:"log_level"`
	Kafka    KafkaConfig `mapstructure:"kafka"`
	DB       DBConfig    `mapstructure:"db"`
}

type TopicConfig struct {
	Topic          string `mapstructure:"topic"`
	GroupID        string `mapstructure:"group_id"`
	BatchSize      int    `mapstructure:"batch_size"`
	BatchMaxWaitMs int    `mapstructure:"batch_max_wait_ms"`
}

type KafkaConfig struct {
	Brokers          []string    `mapstructure:"brokers"`
	BonusInfoBonus   TopicConfig `mapstructure:"bonus_info_bonus"`
	BonusPlayerBonus TopicConfig `mapstructure:"bonus_player_bonus"`
}

type DBConfig struct {
	DSN string `mapstructure:"dsn"`
}

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if envDSN := os.Getenv("DB_DSN"); envDSN != "" {
		cfg.DB.DSN = envDSN
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Env == "" {
		return fmt.Errorf("env is required")
	}

	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[c.LogLevel] {
		return fmt.Errorf("log_level must be one of: debug, info, warn, error")
	}

	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}

	if c.Kafka.BonusInfoBonus.Topic == "" {
		return fmt.Errorf("kafka.bonus_info_bonus.topic is required")
	}
	if c.Kafka.BonusInfoBonus.GroupID == "" {
		return fmt.Errorf("kafka.bonus_info_bonus.group_id is required")
	}
	if c.Kafka.BonusInfoBonus.BatchSize == 0 {
		c.Kafka.BonusInfoBonus.BatchSize = 100
	}
	if c.Kafka.BonusInfoBonus.BatchMaxWaitMs == 0 {
		c.Kafka.BonusInfoBonus.BatchMaxWaitMs = 200
	}

	if c.Kafka.BonusPlayerBonus.Topic == "" {
		return fmt.Errorf("kafka.bonus_player_bonus.topic is required")
	}
	if c.Kafka.BonusPlayerBonus.GroupID == "" {
		return fmt.Errorf("kafka.bonus_player_bonus.group_id is required")
	}
	if c.Kafka.BonusPlayerBonus.BatchSize == 0 {
		c.Kafka.BonusPlayerBonus.BatchSize = 100
	}
	if c.Kafka.BonusPlayerBonus.BatchMaxWaitMs == 0 {
		c.Kafka.BonusPlayerBonus.BatchMaxWaitMs = 200
	}

	if c.DB.DSN == "" {
		return fmt.Errorf("db.dsn is required")
	}
	return nil
}
