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
	"sync"

	"github.com/streadway/amqp"
	"errors"
	rand2 "math/rand"
)

const (
	GroupName string = "BlueForce"
	QueueName string = "BlueForceQueue"
	PublisherMode string = "pub"
	SubscriberMode string = "sub"
	PubSubMode string = "pubsub"
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

//MsgMetaData struct
//Represent the metadata of a message
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
	//log.Printf("Msg ", metaData)
	return msg, nil
}

//MsgStat
//stat for messages
type MsgStat struct {
	receivedMsg int32
	cumulativeDelay int64
}

//publisher routine
func publisher(ch *amqp.Channel, group sync.WaitGroup) {
	defer group.Done()
	imsgSentCount := 0
	for true {
		message, err := createMessageWithMetadata(nodeClientID, uint32(imsgSentCount), time.Now().UnixNano(), *messageSize)
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
}

//subscriber routine
func subscriber(ch *amqp.Channel, group sync.WaitGroup) {
	//declare a non durable queue to receive all the messages
	//for the groupName
	defer group.Done()
	imsgRcvCount := 0
	q, err := ch.QueueDeclare(
		"",    // name
		true,  // durable
		false, // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	failOnError(err, "Failed to declare a queue")
	err = ch.QueueBind(
		q.Name,    // queue name
		"",        // routing key
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
	statmap := make(map[uint32]MsgStat)
	go func() {
		for m := range msgs {

			metaData := parseMetaDataFromMsg(m.Body)
			//if the clientID of the received message
			//is the same of the the local clientId do not increase the stat
			if metaData.clientId != nodeClientID {
				imsgRcvCount++
				//delay for the message in millisecond
				delay := (time.Now().UnixNano() - metaData.timestamp) / 1e6
				statmap[metaData.clientId] = MsgStat{
					receivedMsg:     int32(statmap[metaData.clientId].receivedMsg + 1),
					cumulativeDelay: int64(statmap[metaData.clientId].cumulativeDelay + delay),
				}
				log.Printf(" Received message: clientId %d msgSize %d MsgId %d Total Received messages %d. ReceivedDelay(ms) %d",
					metaData.clientId, len(m.Body), metaData.msgId, imsgRcvCount, delay)
			}
		}
	}()
}


var nodeMode = flag.String("mode", "", "pub for publisher mode sub for subscriber mode and pubsub to launch both")
var brokerAddress = flag.String("broker-address", DefaultBrokerAddress, "The address of the RabbitMQ broker")
var timeSendInterval = flag.Duration("time-send-interval", DefaultTimeSendInterval, "The interval time for the publisher")
var messageSize = flag.Int("message-size", DefaultMessageSize, "Message size")

//create a unique clientID
var nodeClientID uint32 = rand2.Uint32() + uint32(time.Now().Nanosecond())
func main() {
	log.Printf("Started in %s mode with %d clientId", *nodeMode, nodeClientID)
	//Command-line arguments
	flag.Parse()
	var waitgroup sync.WaitGroup
	waitgroup.Add(1)
	if *nodeMode != PublisherMode && *nodeMode != SubscriberMode && *nodeMode != PubSubMode {
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

	if *nodeMode == PublisherMode {
		//launche the publisher goroutines
		go publisher(ch, waitgroup)
	} else if *nodeMode == SubscriberMode {
		//launch the subscriber go routine
		go subscriber(ch, waitgroup)
	} else if  *nodeMode == PubSubMode {
		//start both routines
		//add another routine to the waitGroup
		waitgroup.Add(1)
		go publisher (ch, waitgroup)
		go subscriber (ch, waitgroup)

	} else {
		fmt.Println("Error! bad parameter, exiting")
		os.Exit(-1)
	}
	waitgroup.Wait()
	log.Printf("Exiting")
}
