package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/perocha/consumer/pkg/config"
	"github.com/perocha/consumer/pkg/service"
	"github.com/perocha/goadapters/messaging/eventhub"
	"github.com/perocha/goutils/pkg/telemetry"
)

const (
	SERVICE_NAME = "Consumer"
	configFile   = "config.yaml"
)

func main() {
	// Load the configuration
	cfg, err := config.InitializeConfig()
	if err != nil {
		log.Fatalf("Main::Fatal error::Failed to load configuration %s\n", err.Error())
	}
	// Refresh the configuration with the latest values
	err = cfg.RefreshConfig()
	if err != nil {
		log.Fatalf("Main::Fatal error::Failed to refresh configuration %s\n", err.Error())
	}

	// Initialize App Insights
	telemetryClient, err := telemetry.Initialize(cfg.AppInsightsInstrumentationKey, SERVICE_NAME)
	if err != nil {
		log.Fatalf("Main::Fatal error::Failed to initialize App Insights %s\n", err.Error())
	}
	// Add telemetry object to the context, so that it can be reused across the application
	ctx := context.WithValue(context.Background(), telemetry.TelemetryContextKey, telemetryClient)

	// Initialize EventHub consumer adapter
	consumerInstance, err := eventhub.ConsumerInitializer(ctx, cfg.EventHubNameConsumer, cfg.EventHubConsumerConnectionString, cfg.CheckpointStoreContainerName, cfg.CheckpointStoreConnectionString)
	if err != nil {
		telemetryClient.TrackException(ctx, "Main::Fatal error::Failed to initialize EventHub", err, telemetry.Critical, nil, true)
		log.Fatalf("Main::Fatal error::Failed to initialize EventHub %s\n", err.Error())
	}

	telemetryClient.TrackTrace(ctx, "Main::All adapters initialized successfully", telemetry.Information, nil, true)

	// Create a channel to listen for termination signals
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Initialize service
	serviceInstance := service.Initialize(ctx, consumerInstance)
	go serviceInstance.Start(ctx, signals)

	telemetryClient.TrackTrace(ctx, "Main::Service layer initialized successfully", telemetry.Information, nil, true)

	// Infinite loop
	for {
		select {
		case <-signals:
			telemetryClient.TrackTrace(ctx, "Main::Received termination signal", telemetry.Information, nil, true)
			serviceInstance.Stop(ctx)
			return
		case <-time.After(2 * time.Minute):
			// Do nothing
			log.Println("Main::Waiting for termination signal")
		}
	}
}
