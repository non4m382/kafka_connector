package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/openfaas/faas-provider/auth"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/openfaas/connector-sdk/types"
)

func main() {
	consumeKafkaFull()
}

type ResponseReceiver struct {
}

// Response is triggered by the controller when a message is
// received from the function invocation
func (ResponseReceiver) Response(res types.InvokerResponse) {
	if res.Error != nil {
		log.Printf("[tester] error: %s", res.Error.Error())
	} else {
		log.Printf("[tester] result: [%d] %s => %s (%d) bytes (%fs)", res.Status, res.Topic, res.Function, len(*res.Body), res.Duration.Seconds())
	}
}

func consumeKafkaFull() {
	var (
		// kafka
		kafkaBrokerUrl     string
		kafkaVerbose       bool
		kafkaTopic         string
		kafkaConsumerGroup string
		kafkaClientId      string
		username,
		password,
		gateway string
		interval time.Duration
	)

	//kafka config
	flag.StringVar(&kafkaBrokerUrl, "kafka-brokers", "kafka.dtcsolution.vn:9094", "Kafka brokers in comma separated value")
	flag.BoolVar(&kafkaVerbose, "kafka-verbose", true, "Kafka verbose logging")
	flag.StringVar(&kafkaTopic, "kafka-topic", "create_transaction,send_notify", "Kafka topic. Only one topic per worker.")
	flag.StringVar(&kafkaConsumerGroup, "kafka-consumer-group", "go-group", "Kafka consumer group")
	flag.StringVar(&kafkaClientId, "kafka-client-id", "my-client-id", "Kafka client id")

	//openfaas config
	flag.StringVar(&username, "username", "admin", "username")
	flag.StringVar(&password, "password", "I2aHmJdPCxqc", "password")
	flag.StringVar(&gateway, "gateway", "http://localhost:8088", "gateway")
	flag.DurationVar(&interval, "interval", time.Second*10, "Interval between emitting a sample message")
	flag.Parse()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	brokers := strings.Split(kafkaBrokerUrl, ",")
	topics := strings.Split(kafkaTopic, ",")

	creds := &auth.BasicAuthCredentials{
		User:     username,
		Password: password,
	}

	connectorConfig := &types.ControllerConfig{
		RebuildInterval:         time.Second * 30,
		GatewayURL:              gateway,
		PrintResponse:           true,
		PrintRequestBody:        true,
		PrintResponseBody:       true,
		AsyncFunctionInvocation: false,
		ContentType:             "application/json",
		UserAgent:               "ducnt/kafka-connector",
		UpstreamTimeout:         time.Second * 120,
	}

	fmt.Printf("Tester connector. Topic: %s, Interval: %s\n", kafkaTopic, interval)

	controller := types.NewController(creds, connectorConfig)
	receiver := ResponseReceiver{}
	controller.Subscribe(&receiver)
	controller.BeginMapBuilder()

	additionalHeaders := http.Header{}

	kafkaConfig := kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     kafkaClientId,
		GroupTopics: topics,
		//Topic:           topic,
		MinBytes:        10e3,            // 10KB
		MaxBytes:        10e6,            // 10MB
		MaxWait:         1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
		ReadLagInterval: -1,
	}

	reader := kafka.NewReader(kafkaConfig)
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Panicf("error while receiving message: %s", err.Error())
			continue
		}
		var value = m.Value
		var request = &value
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s\n", m.Topic, m.Partition, m.Offset, string(value))
		log.Printf("[tester] Emitting event on topic %s - %s\n", m.Topic, gateway)
		h := additionalHeaders.Clone()
		controller.Invoke(m.Topic, request, h)
	}

}
