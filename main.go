package main

import (
	"github.com/slimlima/go-infra/queues"
)

func main() {
	topic := "ca_request"
	brokersUrl := []string{"localhost:9092"}
	worker, err := queues.NewKafkaConnectConsumer(brokersUrl)
	if err != nil {
		panic(err)
	}

	queues.NewKafkaWorker(worker, topic, 0)

	if err := worker.Close(); err != nil {
		panic(err)
	}
}
