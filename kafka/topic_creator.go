package main

import (
	"flag"
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

func main() {

	var topicName, kafkaDSN string

	flag.StringVar(
		&kafkaDSN,
		"k",
		"localhost:9094",
		"kafka DSN",
	)
	// ApplicationInformationBus
	flag.StringVar(
		&topicName,
		"t",
		"",
		"topic name",
	)

	flag.Parse()

	if kafkaDSN == "" {
		panic("topic name is required")
	}

	conn, err := kafka.Dial("tcp", "localhost:9094")
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     3,
			ReplicationFactor: 2,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("topic successfully created")
}
