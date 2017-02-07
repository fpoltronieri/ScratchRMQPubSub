package main

import (
	"fmt"
	"log"
	"os"
	"crypto/rand"
	"io"
	"flag"
	"time"
	"encoding/binary"
	"bytes"

	"github.com/streadway/amqp"
	"errors"
)

const (
	GroupName string = "BlueForce"
	QueueName string = "BlueForceQueue"
	PublisherMode string = "pub"
	SubscriberMode string = "sub"
	DefaultBrokerAddress string = "localhost"
	BrokerPort int = 5672
	DefaultTimeSendInterval time.Duration = 1000
	DefaultMessageSize int = 1024
	MetaDataSize int = 12
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type MsgMetaData struct {
	clientId uint32
	msgId uint32
	timestamp int64
}
//the function parses the metadata from a []byte message and returns
//a MsgMetaData struct
func parseMetaDataFromMsg(msg []byte) (MsgMetaData) {
	msgMetaData := MsgMetaData{ clientId: binary.BigEndian.Uint32(msg[:4]),
		msgId: binary.BigEndian.Uint32(msg[4:8]),
		timestamp: int64(binary.BigEndian.Uint64(msg[8:16])),
	}
	log.Printf("parsed metadata", msgMetaData)
	return msgMetaData
}


func createMessageWithMetadata(clientId uint32, msgId uint32, timestamp int64, msgLen int) ([]byte, error) {
	msgMetaData := new(bytes.Buffer)
	binary.Write(msgMetaData, binary.BigEndian, clientId)
	binary.Write(msgMetaData, binary.BigEndian, msgId)
	binary.Write(msgMetaData, binary.BigEndian, timestamp)
	metaData := msgMetaData.Bytes()
	if msgLen <= len(metaData) {
		return nil, errors.New("The selected metaData len is to shoort")
	}
	msgData := make([]byte, msgLen - len(metaData))
	io.ReadFull(rand.Reader, msgData)
	msg := append(metaData, msgData...)
	log.Printf("Msg ", metaData)
	return msg, nil
}

type messageStat struct {
	receivedMsg int32
	cumulativeDelay int64
}

func main() {
	//Command-line arguments
	nodeMode := flag.String("mode", "", "pub for publisher mode," +
		"sub for subscriber mode")
	brokerAddress := flag.String("broker-address", DefaultBrokerAddress, "The address of the RabbitMQ broker")
	timeSendInterval := flag.Duration("time-send-interval", DefaultTimeSendInterval, "The interval time for the publisher")
	messageSize := flag.Int("message-size", DefaultMessageSize, "Message size")
	flag.Parse()
	if *nodeMode != PublisherMode && *nodeMode != SubscriberMode {
		fmt.Println("Error! bad parameter, exiting")
		flag.Usage()
		os.Exit(-1)
	}
	//connecting to the broker
	conn, err := amqp.Dial("amqp://guest:guest@" + *brokerAddress + ":5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		GroupName,   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	//declare the message count
	imsgSentCount := 0
	imsgRcvCount := 0
	if *nodeMode == PublisherMode {
		//create a 1024 random byte message
		for true {
			message, err := createMessageWithMetadata(1, uint32(imsgSentCount), time.Now().UnixNano(), *messageSize)
			if err != nil {
				log.Printf("Impossible to create the message")
				os.Exit(-1)
			}
			err = ch.Publish(
				GroupName, // exchange
				"",          // routing key
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					Body:        message,
				})
			failOnError(err, "Failed to publish a message")
			if err == nil {
				imsgSentCount++
			}
			log.Printf("Sent message: msgSize %d sent messages %d metadata", len(message), imsgSentCount,
			parseMetaDataFromMsg(message))
			time.Sleep((*timeSendInterval) *  time.Millisecond)
		}
	} else if *nodeMode == SubscriberMode {
		//declare a non durable queue to receive all the messages
		//for the groupName
		q, err := ch.QueueDeclare(
			"",    // name
			true, // durable
			false, // delete when usused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)

		failOnError(err, "Failed to declare a queue")
		err = ch.QueueBind(
			q.Name, // queue name
			"",     // routing key
			GroupName, // exchange
			false,
			nil)
		failOnError(err, "Failed to bind a queue")

		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		failOnError(err, "Failed to register a consumer")

		//create the map to store the messages
		statmap := make(map[uint32]messageStat)
		forever := make(chan bool)
		go func() {
			for m := range msgs {
				imsgRcvCount++
				//delay for the message in millisecond
				metaData := parseMetaDataFromMsg(m.Body)
				delay := (time.Now().UnixNano() - metaData.timestamp) / 1e6
				statmap[metaData.clientId] = messageStat{
							receivedMsg: int32(statmap[metaData.clientId].receivedMsg + 1),
							cumulativeDelay: int64(statmap[metaData.clientId].cumulativeDelay + delay),
				}
				log.Printf(" Received message: msgSize %d MsgId %s Total Received messages %d. Delay(ms) %d", len(m.Body),
					metaData.msgId, imsgRcvCount, delay)
			}
		}()
		<-forever
	} else {
		fmt.Println("Error! bad parameter, exiting")
		os.Exit(-1)
	}
}
