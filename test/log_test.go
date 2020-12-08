package test

import (
	"testing"
	"time"

	"gitlab.bcowtech.de/bcow-go/log"
	kafka "gitlab.bcowtech.de/bcow-go/log-forwarder-kafka"
)

func TestLog(t *testing.T) {
	forwarder := kafka.NewForwarder(&kafka.Option{
		Topics:            []string{"myTopic"},
		ShutdownTimeoutMs: int(15 * time.Second),
		ConfigMap: &kafka.ConfigMap{
			"client.id":                "demo",
			"bootstrap.servers":        BootstrapServers,
			"enable.idempotence":       "true",
			"message.send.max.retries": "15",
		},
	})

	defer forwarder.Close()

	logger := log.NewLogger(&log.Config{
		Category: "demo-forwarder-kafka",
		Source:   "192.168.56.51",
		Version:  "v1.0.1",
		Writer: &log.PlainTextWriter{
			Stream: forwarder,
		},
	})

	logger.Write(log.NOTICE, "log notice message")
}
