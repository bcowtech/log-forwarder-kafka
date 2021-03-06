package test

import (
	"os"
	"testing"
	"time"

	kafka "github.com/bcowtech/log-forwarder-kafka"
)

var (
	BootstrapServers = os.Getenv("BOOTSTRAP_SERVERS")
)

func TestForwarder(t *testing.T) {
	forwarder := kafka.NewForwarder(&kafka.Option{
		Topics:            []string{"myTopic"},
		ShutdownTimeoutMs: int(15 * time.Second),
		ConfigMap: &kafka.ConfigMap{
			/*
			 * you may add the following config:
			 *  bootstrap.servers
			 *  enable.idempotence
			 *  message.send.max.retries
			 *  partitioner
			 *  compression.type
			 *  security.protocol
			 *  request.required.acks   ; must greater than min.insync.replicas
			 */
			"client.id":                "demo",
			"bootstrap.servers":        BootstrapServers,
			"enable.idempotence":       "true",
			"message.send.max.retries": "15",
		},
	})

	defer forwarder.Close()

	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		forwarder.Write([]byte(word))
	}
}
