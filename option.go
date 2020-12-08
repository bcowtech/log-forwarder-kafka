package kafka

import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

type (
	ConfigMap = kafka.ConfigMap

	Option struct {
		Topics            []string
		ShutdownTimeoutMs int
		ConfigMap         *ConfigMap
	}
)
