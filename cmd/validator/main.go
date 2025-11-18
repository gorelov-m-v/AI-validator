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
		zap.String("topic", cfg.Kafka.Topic),
		zap.String("group_id", cfg.Kafka.GroupID),
		zap.Int("batch_size", cfg.Kafka.BatchSize),
		zap.Int("batch_max_wait_ms", cfg.Kafka.BatchMaxWaitMs),
	)

	db, err := database.New(cfg.DB.DSN, logger)
	if err != nil {
		logger.Fatal("failed to initialize database", zap.Error(err))
	}
	defer db.Close()

	repo := database.NewRepository(db, logger)

	consumer, err := kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID, logger)
	if err != nil {
		logger.Fatal("failed to initialize kafka consumer", zap.Error(err))
	}
	defer consumer.Close()

	proc := processor.New(cfg.Env, db, repo, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	var messageCount atomic.Int64
	batchMaxWait := time.Duration(cfg.Kafka.BatchMaxWaitMs) * time.Millisecond

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Warn("stopping message processing")
				return
			case <-ticker.C:
				count := messageCount.Load()
				logger.Info("processing stats",
					zap.Int64("messages_processed", count),
					zap.Int64("db_errors", proc.GetErrorCount()),
				)
			default:
				batch, err := consumer.ReadBatch(ctx, cfg.Kafka.BatchSize, batchMaxWait)
				if err != nil {
					if ctx.Err() != nil {
						// Context cancelled, exit gracefully
						return
					}
					logger.Error("failed to read kafka batch",
						zap.Error(err),
						zap.String("topic", cfg.Kafka.Topic),
						zap.Int("max_batch_size", cfg.Kafka.BatchSize),
						zap.Duration("max_wait", batchMaxWait),
					)
					time.Sleep(time.Second) // Backoff on error
					continue
				}

				if len(batch) == 0 {
					continue
				}

				logger.Debug("processing batch",
					zap.Int("batch_size", len(batch)),
				)

				successfulMessages := make([]*kafka.Message, 0, len(batch))
				for _, msg := range batch {
					if err := proc.ProcessMessage(ctx, msg); err != nil {
						logger.Error("failed to process message",
							zap.Error(err),
							zap.String("topic", msg.Topic),
							zap.Int("partition", msg.Partition),
							zap.Int64("offset", msg.Offset),
						)
						break
					}

					successfulMessages = append(successfulMessages, msg)
					messageCount.Add(1)
				}

				if len(successfulMessages) > 0 {
					if err := consumer.CommitBatch(ctx, successfulMessages); err != nil {
						logger.Error("failed to commit batch",
							zap.Error(err),
							zap.Int("messages", len(successfulMessages)),
						)
					}
				}
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
		zap.Int64("total_messages_processed", messageCount.Load()),
		zap.Int64("total_db_errors", proc.GetErrorCount()),
	)
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
