package servicebus

import (
	"fmt"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func NewClient(connectionString, namespace string) (*azservicebus.Client, error) {
	if connectionString != "" {
		slog.Info("connecting to Service Bus with connection string")
		return azservicebus.NewClientFromConnectionString(connectionString, nil)
	}

	if namespace == "" {
		return nil, fmt.Errorf("either SERVICEBUS_CONNECTION_STRING or SERVICEBUS_NAMESPACE must be set")
	}

	slog.Info("connecting to Service Bus with managed identity", "namespace", namespace)
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("creating azure credential: %w", err)
	}

	return azservicebus.NewClient(namespace, cred, nil)
}
