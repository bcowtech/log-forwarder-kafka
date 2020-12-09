package kafka

import (
	"log"
	"os"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	defaultLogger = log.New(os.Stdout, "[bcow-go/log-forwarder-kafka]", log.LstdFlags|log.Lmicroseconds|log.Llongfile|log.Lmsgprefix)
)

type Forwarder struct {
	producer *kafka.Producer

	topics            []string
	shutdownTimeoutMs int
	logger            *log.Logger
}

func NewForwarder(opt *Option) *Forwarder {
	p, err := kafka.NewProducer(opt.ConfigMap)
	if err != nil {
		panic(err)
	}

	instance := &Forwarder{
		producer:          p,
		topics:            opt.Topics,
		shutdownTimeoutMs: opt.ShutdownTimeoutMs,
		logger:            defaultLogger,
	}
	instance.listenEvents()
	return instance
}

func (f *Forwarder) Write(data []byte) (n int, err error) {
	p := f.producer
	for _, topic := range f.topics {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          data,
		}, nil)
		if err != nil {
			break
		}
		// Wait for message deliveries before shutting down
		p.Flush(f.shutdownTimeoutMs)
	}
	return len(data), err
}

func (f *Forwarder) Close() {
	p := f.producer

	defer p.Close()

}

func (f *Forwarder) listenEvents() {
	p := f.producer

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					err, ok := ev.TopicPartition.Error.(*kafka.Error)
					// TODO: save key & value to separater file, maybe using xdr https://github.com/davecgh/go-xdr
					if ok {
						f.logger.Printf("(%s) {Code:%d IsFatal:%t IsRetriable:%t TxnRequiresAbort:%t}: %v\n",
							ev.TopicPartition,
							err.Code(),
							err.IsFatal(),
							err.IsRetriable(),
							err.TxnRequiresAbort(),
							ev.TopicPartition.Error)
					} else {
						f.logger.Printf("(%s) (%T): %[2]v\n",
							ev.TopicPartition,
							ev.TopicPartition.Error)
					}
				}
			case kafka.Error:
				f.logger.Printf("{Code:%d IsFatal:%t IsRetriable:%t TxnRequiresAbort:%t}: %v\n",
					ev.Code(),
					ev.IsFatal(),
					ev.IsRetriable(),
					ev.TxnRequiresAbort(),
					ev.Error())
			}
		}
	}()
}
