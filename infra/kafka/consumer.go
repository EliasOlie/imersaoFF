package kafka

import (
	"fmt"
	"log"
	"os"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	MsgChannel chan *ckafka.Message
}

func NewKafkaConsumer(msgChannel chan *ckafka.Message) *KafkaConsumer {
	return &KafkaConsumer{MsgChannel: msgChannel}
}

func (k *KafkaConsumer) Consume() {
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KafkaBoostrapServers"),
		"group.id":          os.Getenv("KafkaConsumerGroupId"),
	}

	c, err := ckafka.NewConsumer(configMap)
	if err != nil {
		log.Fatal("Error consuming kafka messages" + err.Error())
	}

	topics := []string{os.Getenv("KafkaReadTopics")}
	c.SubscribeTopics(topics, nil)
	fmt.Println("Kafka c Started")

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			k.MsgChannel <- msg
		}
	}
}
