package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"AI-validator/internal/config"
	"AI-validator/internal/database"
	"AI-validator/internal/kafka"
	"AI-validator/internal/processor"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	configPath = flag.String("config", "config.yaml", "path to configuration file")
	version    = "1.0.0"
)

func main() {
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger, err := initLogger(cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("starting AI-validator",
		zap.String("version", version),
		zap.String("config", *configPath),
		zap.String("log_level", cfg.LogLevel),
	)

	logger.Info("configuration loaded",
		zap.String("env", cfg.Env),
		zap.String("bonus_info_bonus_topic", cfg.Kafka.BonusInfoBonus.Topic),
		zap.String("bonus_info_bonus_group", cfg.Kafka.BonusInfoBonus.GroupID),
		zap.String("bonus_player_bonus_topic", cfg.Kafka.BonusPlayerBonus.Topic),
		zap.String("bonus_player_bonus_group", cfg.Kafka.BonusPlayerBonus.GroupID),
	)

	db, err := database.New(cfg.DB.DSN, logger)
	if err != nil {
		logger.Fatal("failed to initialize database", zap.Error(err))
	}
	defer db.Close()

	repo := database.NewRepository(db, logger)

	bonusInfoConsumer, err := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.BonusInfoBonus.Topic,
		cfg.Kafka.BonusInfoBonus.GroupID,
		logger,
	)
	if err != nil {
		logger.Fatal("failed to initialize bonus_info_bonus kafka consumer", zap.Error(err))
	}
	defer bonusInfoConsumer.Close()

	playerBonusConsumer, err := kafka.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.BonusPlayerBonus.Topic,
		cfg.Kafka.BonusPlayerBonus.GroupID,
		logger,
	)
	if err != nil {
		logger.Fatal("failed to initialize bonus_player_bonus kafka consumer", zap.Error(err))
	}
	defer playerBonusConsumer.Close()

	bonusInfoProcessor := processor.New(cfg.Env, db, repo, logger)
	playerBonusProcessor := processor.NewPlayerBonusProcessor(cfg.Env, repo, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	var bonusInfoMessageCount atomic.Int64
	var playerBonusMessageCount atomic.Int64

	go runConsumer(
		ctx,
		bonusInfoConsumer,
		bonusInfoProcessor,
		cfg.Env,
		cfg.Kafka.BonusInfoBonus.BatchSize,
		time.Duration(cfg.Kafka.BonusInfoBonus.BatchMaxWaitMs)*time.Millisecond,
		&bonusInfoMessageCount,
		"bonus_info_bonus",
		logger,
	)

	go runConsumer(
		ctx,
		playerBonusConsumer,
		playerBonusProcessor,
		cfg.Env,
		cfg.Kafka.BonusPlayerBonus.BatchSize,
		time.Duration(cfg.Kafka.BonusPlayerBonus.BatchMaxWaitMs)*time.Millisecond,
		&playerBonusMessageCount,
		"bonus_player_bonus",
		logger,
	)

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logger.Info("processing stats",
					zap.Int64("bonus_info_bonus_messages", bonusInfoMessageCount.Load()),
					zap.Int64("bonus_info_bonus_errors", bonusInfoProcessor.GetErrorCount()),
					zap.Int64("bonus_player_bonus_messages", playerBonusMessageCount.Load()),
					zap.Int64("bonus_player_bonus_errors", playerBonusProcessor.GetErrorCount()),
				)
			}
		}
	}()

	sig := <-sigChan
	logger.Warn("received shutdown signal",
		zap.String("signal", sig.String()),
	)

	cancel()

	time.Sleep(2 * time.Second)

	logger.Warn("shutdown complete",
		zap.Int64("bonus_info_bonus_processed", bonusInfoMessageCount.Load()),
		zap.Int64("bonus_info_bonus_errors", bonusInfoProcessor.GetErrorCount()),
		zap.Int64("bonus_player_bonus_processed", playerBonusMessageCount.Load()),
		zap.Int64("bonus_player_bonus_errors", playerBonusProcessor.GetErrorCount()),
	)
}

type messageProcessor interface {
	ProcessMessage(ctx context.Context, msg *kafka.Message) processor.ProcessingResult
}

func runConsumer(
	ctx context.Context,
	consumer *kafka.Consumer,
	proc messageProcessor,
	env string,
	batchSize int,
	batchMaxWait time.Duration,
	messageCount *atomic.Int64,
	consumerName string,
	logger *zap.Logger,
) {
	for {
		if ctx.Err() != nil {
			logger.Warn("stopping consumer", zap.String("consumer", consumerName))
			return
		}

		batch, err := consumer.ReadBatch(ctx, batchSize, batchMaxWait)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, exit gracefully
				return
			}
			logger.Error("failed to read kafka batch",
				zap.Error(err),
				zap.String("consumer", consumerName),
				zap.Int("max_batch_size", batchSize),
				zap.Duration("max_wait", batchMaxWait),
			)
			time.Sleep(time.Second)
			continue
		}

		if len(batch) == 0 {
			continue
		}

		logger.Debug("processing batch",
			zap.String("consumer", consumerName),
			zap.Int("batch_size", len(batch)),
		)

		type partitionState struct {
			hasInfraError bool
			lastMessage   *kafka.Message
		}
		partitionStates := make(map[int]*partitionState)

		var okCount, domainErrorCount, duplicateCount, infraErrorCount int

		for _, msg := range batch {
			state, exists := partitionStates[msg.Partition]
			if !exists {
				state = &partitionState{}
				partitionStates[msg.Partition] = state
			}

			if state.hasInfraError {
				logger.Debug("skipping message due to previous infra error in partition",
					zap.String("consumer", consumerName),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
				)
				continue
			}

			result := proc.ProcessMessage(ctx, msg)

			switch result.Type {
			case processor.ResultOK:
				logger.Debug("message processed successfully",
					zap.String("consumer", consumerName),
					zap.String("env", env),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
				)
				okCount++
				state.lastMessage = msg

			case processor.ResultDomainError:
				logger.Warn("message processed with domain error",
					zap.String("consumer", consumerName),
					zap.String("env", env),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.String("reason", result.Message),
				)
				domainErrorCount++
				state.lastMessage = msg

			case processor.ResultDuplicateOffset:
				logger.Debug("duplicate offset skipped",
					zap.String("consumer", consumerName),
					zap.String("env", env),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
				)
				duplicateCount++
				state.lastMessage = msg

			case processor.ResultInfraError:
				logger.Error("infra error during message processing",
					zap.Error(result.Err),
					zap.String("consumer", consumerName),
					zap.String("env", env),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.String("reason", result.Message),
				)
				infraErrorCount++
				state.hasInfraError = true
			}
		}

		messagesToCommit := make([]*kafka.Message, 0, len(partitionStates))
		for _, state := range partitionStates {
			if state.lastMessage != nil {
				messagesToCommit = append(messagesToCommit, state.lastMessage)
			}
		}

		if len(messagesToCommit) > 0 {
			if err := consumer.CommitBatch(ctx, messagesToCommit); err != nil {
				logger.Error("failed to commit batch",
					zap.Error(err),
					zap.String("consumer", consumerName),
					zap.Int("messages", len(messagesToCommit)),
				)
			} else {
				logger.Info("batch processed and committed",
					zap.String("consumer", consumerName),
					zap.Int("total_messages", len(batch)),
					zap.Int("ok", okCount),
					zap.Int("domain_errors", domainErrorCount),
					zap.Int("duplicates", duplicateCount),
					zap.Int("infra_errors", infraErrorCount),
					zap.Int("committed_partitions", len(messagesToCommit)),
				)
				messageCount.Add(int64(okCount + domainErrorCount + duplicateCount))
			}
		} else {
			logger.Warn("no messages to commit in batch",
				zap.String("consumer", consumerName),
				zap.Int("total_messages", len(batch)),
				zap.Int("infra_errors", infraErrorCount),
			)
		}
	}
}

func initLogger(logLevel string) (*zap.Logger, error) {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	var level zapcore.Level
	switch logLevel {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}
	config.Level = zap.NewAtomicLevelAt(level)

	return config.Build()
}
