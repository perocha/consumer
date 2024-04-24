package service

import (
	"context"
	"os"

	"github.com/perocha/goadapters/messaging/message"
	"github.com/perocha/goutils/pkg/telemetry"
)

// ServiceImpl is a struct implementing the Service interface.
type ServiceImpl struct {
	consumerInstance message.MessagingSystem
}

// Creates a new instance of ServiceImpl.
func Initialize(ctx context.Context, consumer message.MessagingSystem) *ServiceImpl {
	telemetryClient := telemetry.GetTelemetryClient(ctx)
	telemetryClient.TrackTrace(ctx, "services::Initialize::Initializing service logic", telemetry.Information, nil, true)

	consumerInstance := consumer

	return &ServiceImpl{
		consumerInstance: consumerInstance,
	}
}

// Starts listening for incoming events.
func (s *ServiceImpl) Start(ctx context.Context, signals <-chan os.Signal) error {
	telemetryClient := telemetry.GetTelemetryClient(ctx)

	channel, cancelCtx, err := s.consumerInstance.Subscribe(ctx)
	if err != nil {
		telemetryClient.TrackException(ctx, "services::Start::Failed to subscribe to events", err, telemetry.Critical, nil, true)
		return err
	}

	telemetryClient.TrackTrace(ctx, "services::Start::Subscribed to events", telemetry.Information, nil, true)

	for {
		select {
		case message := <-channel:
			// Update the context with the operation ID
			ctx = context.WithValue(ctx, telemetry.OperationIDKeyContextKey, message.GetOperationID())

			if message.GetError() == nil {
				// If we reach this point, we have a valid event. Process it!!!
				// Log in console content of eventData
				telemetryProps := map[string]string{
					"OperationID": message.GetOperationID(),
					"Status":      message.GetStatus(),
					"Command":     message.GetCommand(),
				}

				telemetryClient.TrackTrace(ctx, "services::ProcessEvent::Processing event", telemetry.Information, telemetryProps, true)
			} else {
				// Error received. In this case we'll discard message but report an exception
				properties := map[string]string{
					"Error": message.GetError().Error(),
				}
				telemetryClient.TrackException(ctx, "services::Start::Error processing message", message.GetError(), telemetry.Error, properties, true)
			}
		case <-ctx.Done():
			telemetryClient.TrackTrace(ctx, "services::Start::Context canceled. Stopping event listener.", telemetry.Information, nil, true)
			cancelCtx()
			s.consumerInstance.Close(ctx)
			return nil
		case <-signals:
			telemetryClient.TrackTrace(ctx, "services::Start::Received termination signal", telemetry.Information, nil, true)
			cancelCtx()
			s.consumerInstance.Close(ctx)
			return nil
		}
	}
}

// Stop the service
func (s *ServiceImpl) Stop(ctx context.Context) {
	telemetryClient := telemetry.GetTelemetryClient(ctx)
	telemetryClient.TrackTrace(ctx, "services::Stop::Stopping service", telemetry.Information, nil, true)

	s.consumerInstance.Close(ctx)
}
