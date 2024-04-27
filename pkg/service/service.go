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
	xTelemetry := telemetry.GetXTelemetryClient(ctx)
	xTelemetry.Info(ctx, "services::Initialize::Initializing service layer")

	consumerInstance := consumer

	return &ServiceImpl{
		consumerInstance: consumerInstance,
	}
}

// Starts listening for incoming events.
func (s *ServiceImpl) Start(ctx context.Context, signals <-chan os.Signal) error {
	xTelemetry := telemetry.GetXTelemetryClient(ctx)

	channel, cancelCtx, err := s.consumerInstance.Subscribe(ctx)
	if err != nil {
		xTelemetry.Error(ctx, "services::Start::Failed to subscribe to events", telemetry.String("error", err.Error()))
		return err
	}

	xTelemetry.Info(ctx, "services::Start::Subscribed to events")

	for {
		select {
		case message := <-channel:
			// Update the context with the operation ID
			ctx = context.WithValue(ctx, telemetry.OperationIDKeyContextKey, message.GetOperationID())

			if message.GetError() == nil {
				// If we reach this point, we have a valid event. Process it!!!
				// Log in console content of eventData
				xTelemetry.Info(ctx, "services::ProcessEvent::Processing event", telemetry.String("Status", message.GetStatus()), telemetry.String("Command", message.GetCommand()))
			} else {
				xTelemetry.Error(ctx, "services::Start::Error processing message", telemetry.String("error", message.GetError().Error()))
			}
		case <-ctx.Done():
			xTelemetry.Info(ctx, "services::Start::Context canceled. Stopping event listener")
			cancelCtx()
			s.consumerInstance.Close(ctx)
			return nil
		case <-signals:
			xTelemetry.Info(ctx, "services::Start::Received termination signal")
			cancelCtx()
			s.consumerInstance.Close(ctx)
			return nil
		}
	}
}

// Stop the service
func (s *ServiceImpl) Stop(ctx context.Context) {
	xTelemetry := telemetry.GetXTelemetryClient(ctx)
	xTelemetry.Info(ctx, "services::Stop::Stopping service")

	s.consumerInstance.Close(ctx)
}
