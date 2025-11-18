package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

var (
	broker = flag.String("broker", "localhost:9092", "Kafka broker address")
	topic  = flag.String("topic", "_bonus.v2.info.Bonus", "Kafka topic")
	count  = flag.Int("count", 10, "Number of messages to produce")
)

type BonusMessage struct {
	Message struct {
		EventType string `json:"eventType"`
	} `json:"message"`
	Bonus struct {
		NodeID            string `json:"nodeId"`
		ID                string `json:"id"`
		BonusID           string `json:"bonusId"`
		BonusCategory     string `json:"bonusCategory"`
		PlayerID          string `json:"playerId"`
		Currency          string `json:"currency"`
		ReceivedBalance   string `json:"receivedBalance"`
		Balance           string `json:"balance"`
		Wager             string `json:"wager"`
		TotalWager        string `json:"totalWager"`
		PlayerBonusStatus int    `json:"playerBonusStatus"`
		ExpiredAt         string `json:"expiredAt"`
		UpdatedAt         int64  `json:"updatedAt"`
		CreatedAt         int64  `json:"createdAt"`
		IsWager           int    `json:"isWager"`
	} `json:"bonus"`
}

func main() {
	flag.Parse()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{*broker},
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	log.Printf("Producing %d messages to topic %s on broker %s\n", *count, *topic, *broker)

	for i := 0; i < *count; i++ {
		msg := generateBonusMessage()
		data, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Failed to marshal message: %v\n", err)
			continue
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Value: data,
		})

		if err != nil {
			log.Printf("Failed to write message: %v\n", err)
		} else {
			log.Printf("✓ Message %d sent: seq_key=%s, event=%s\n", i+1, msg.Bonus.ID, msg.Message.EventType)
		}

		time.Sleep(100 * time.Millisecond)
	}

	log.Println("Done!")
}

func generateBonusMessage() BonusMessage {
	eventTypes := []string{
		"playerBonusCreate",
		"playerBonusUpdate",
		"playerBonusActivate",
		"playerBonusCancel",
		"playerBonusComplete",
	}

	categories := []string{"casino", "sport", "live"}
	currencies := []string{"EUR", "USD", "GBP"}

	now := time.Now().Unix()

	var msg BonusMessage
	msg.Message.EventType = eventTypes[time.Now().UnixNano()%int64(len(eventTypes))]
	msg.Bonus.NodeID = uuid.New().String()
	msg.Bonus.ID = uuid.New().String()
	msg.Bonus.BonusID = uuid.New().String()
	msg.Bonus.BonusCategory = categories[time.Now().UnixNano()%int64(len(categories))]
	msg.Bonus.PlayerID = uuid.New().String()
	msg.Bonus.Currency = currencies[time.Now().UnixNano()%int64(len(currencies))]
	msg.Bonus.ReceivedBalance = "100.00"
	msg.Bonus.Balance = "100.00"
	msg.Bonus.Wager = "0"
	msg.Bonus.TotalWager = "500.00"
	msg.Bonus.PlayerBonusStatus = 1
	msg.Bonus.ExpiredAt = fmt.Sprintf("%d", now+86400*7) // 7 days
	msg.Bonus.UpdatedAt = now
	msg.Bonus.CreatedAt = now
	msg.Bonus.IsWager = 1

	return msg
}
