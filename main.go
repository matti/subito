package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	done := make(chan bool, 1)
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		log.Println("got signal", <-sigs)
		done <- false
	}()

	natsServers := os.Args[1]
	natsPingInterval, err := time.ParseDuration(os.Args[2])
	if err != nil {
		log.Fatalln("natsPingInterval", err)
	}

	natsMaxPingsOutstanding, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalln("natsMaxPingsOutstanding", err)
	}

	natsSubject := os.Args[4]
	natsMode := os.Args[5]

	log.Println("start")
	nc, err := nats.Connect(natsServers,
		nats.Timeout(1*time.Second),
		nats.NoReconnect(),
		nats.MaxPingsOutstanding(natsMaxPingsOutstanding),
		nats.PingInterval(natsPingInterval),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Println("disconnected")
			done <- false
		}),
	)

	if err != nil {
		log.Fatalln("nats.Connect", err)
	}

	if nc.ConnectedAddr() == "" {
		log.Println("connect failed to ", natsServers)
		os.Exit(1)
	}

	log.Println("connected", nc.ConnectedAddr())

	switch natsMode {
	case "publish":
		message := os.Args[6]

		log.Println("publishing to", natsSubject, "message", message)
		m := &nats.Msg{
			Subject: natsSubject,
			Data:    []byte(message),
		}

		if err := nc.PublishMsg(m); err != nil {
			log.Println("publish error", err)
			done <- false
		} else if err := nc.Flush(); err != nil {
			log.Println("flush error", err)
			done <- false
		} else {
			done <- true
		}
	case "subscribe":
		subscribePrinted := make(chan bool, 1)
		nc.Subscribe(natsSubject, func(m *nats.Msg) {
			<-subscribePrinted
			log.Println("message", string(m.Data))
			fmt.Println("MESSAGE")
			fmt.Println(string(m.Data))
			done <- true
		})

		log.Println("subscribed", natsSubject)
		fmt.Println("SUBSCRIBED")
		subscribePrinted <- true

	default:
		log.Fatalln("unknown natsMode", natsMode)
	}

	success := <-done
	if success {
		log.Println("done success")
		os.Exit(0)
	} else {
		log.Println("done failure")
		os.Exit(1)
	}
}
