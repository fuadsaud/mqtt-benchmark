package main

import (
	"fmt"
	"log"
	"time"
)

import (
	"github.com/GaryBoone/GoStats/stats"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Client struct {
	ID         int
	BrokerURL  string
	BrokerUser string
	BrokerPass string
	MsgTopic   string
	MsgSize    int
	MsgCount   int
	MsgQoS     byte
	Quiet      bool
}

func (c *Client) Run(res chan *RunResults) {
	newMsgs := make(chan *Message)
	pubMsgs := make(chan *Message)
	doneGen := make(chan bool)
	donePub := make(chan bool)
	runResults := new(RunResults)

	started := time.Now()
	// start generator
	go c.genMessages(newMsgs, doneGen)
	// start publisher
	go c.pubMessages(newMsgs, pubMsgs, doneGen, donePub)

	runResults.ID = c.ID
	times := []float64{}
	for {
		select {
		case m := <-pubMsgs:
			if m.Error {
				log.Printf("CLIENT %v ERROR publishing message: %v: at %v\n", c.ID, m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				// log.Printf("Message published: %v: sent: %v delivered: %v flight time: %v\n", m.Topic, m.Sent, m.Delivered, m.Delivered.Sub(m.Sent))
				runResults.Successes++
				times = append(times, m.Delivered.Sub(m.Sent).Seconds()*1000) // in milliseconds
			}
		case <-donePub:
			// calculate results
			duration := time.Now().Sub(started)
			runResults.MsgTimeMin = stats.StatsMin(times)
			runResults.MsgTimeMax = stats.StatsMax(times)
			runResults.MsgTimeMean = stats.StatsMean(times)
			runResults.MsgTimeStd = stats.StatsSampleStandardDeviation(times)
			runResults.RunTime = duration.Seconds()
			runResults.MsgsPerSec = float64(runResults.Successes) / duration.Seconds()

			// report results and exit
			res <- runResults
			return
		}
	}
}

func (c *Client) genMessages(ch chan *Message, done chan bool) {
	payloadStr := "YjcJGyg09W9JCnpEXtQ6yYzs41XdXFc60UOBkBuLWD0LSjzKyBGz3KdFg7L5OOOo2r4KJU6KtJXv6ZlqCiqMu8GDK56C1OPbxPzWcQ3dcfMDpEr8K4e4oJHNEJvwH1V0JOvVlASucM0SH3uAwpiOLOpaWC7MA22IQgVfEHduJUVyeepbEnA8FYlpItwnFolx2YgtT7zlSpHK8JdMOa0cIO5XfL3oEG5SXaKdSbaBBRGeah3uXgYyXuZIE8ZlSyKgs9BCIl0Yg01dZmyYk8WibC8BQnScJIjiA2h1epJUrqSldMdgdm5cRRTMZvcQsej3algsRbOtny3lqYrUOys4iJsy1pZjIJIBsgomrvowl5Gg9T575df6O9MQ8tKpzY2KUXRts5MRBd6tr9sQ9yUb81oKlmZq0BvgRPgxMFvJ6mn79Ilz25XtFWchVuD5pOCjLM5zfyOYTrQUjFcodSGjO8gDDOIxiYoiZJ8G8o5wiTDLXoKsjoQeMVFHhxxcpcOBsB7zKXXBFnQlHHiSduate5wGRPEp5jpsaahqHV72pqO0kjStZj5m6BFnPXhD0qdJLgkOiwGn7mMzX1A1VrKwPfmt2c0w8DhWHVQdc09ytdE9qD33oThxtWZC3UIGWH347z1v1PoZiyiQQFzE2FOByRnCPzjWY88fgsSDAzKMiEUcbkTPdujHZVSCjDwpDakp9QdSHCrXKxrTrbFPIb3Se8TFR1ECNNrf4pwR7bYu4TdkYUIsQ21x9nyWsvnuVoFTgKZQKerK1gv5p5rOB4DSPIZQTKEHjiS2qUdST3EA9uZpvaielnOUk97NHIVp4I7HFmx0mLZfxP3rQOooakJN3GS2kwF4HWLnhwYGjRftT2mlOPCgflF8Mk06DrHz5kkyadlPJLCFKlKgfYTzF4ir46jGhwYSUcfKBgWLgUOqZnXtlyh7jPXGQjwApwJPxkYLvXfbca4Zke5UriGUSDxqOzr9foLUXUlN4Fs3EJPr"

	for i := 0; i < c.MsgCount; i++ {
		ch <- &Message{
			Topic:   c.MsgTopic,
			QoS:     c.MsgQoS,
			Payload: []byte(payloadStr),
		}
	}
	done <- true
	// log.Printf("CLIENT %v is done generating messages\n", c.ID)
	return
}

func (c *Client) pubMessages(in, out chan *Message, doneGen, donePub chan bool) {
	onConnected := func(client mqtt.Client) {
		if !c.Quiet {
			log.Printf("CLIENT %v is connected to the broker %v\n", c.ID, c.BrokerURL)
		}
		ctr := 0
		for {
			select {
			case m := <-in:
				m.Sent = time.Now()
				token := client.Publish(m.Topic, m.QoS, false, m.Payload)
				token.Wait()
				if token.Error() != nil {
					log.Printf("CLIENT %v Error sending message: %v\n", c.ID, token.Error())
					m.Error = true
				} else {
					m.Delivered = time.Now()
					m.Error = false
				}
				out <- m

				if ctr > 0 && ctr%100 == 0 {
					if !c.Quiet {
						log.Printf("CLIENT %v published %v messages and keeps publishing...\n", c.ID, ctr)
					}
				}
				ctr++
			case <-doneGen:
				donePub <- true
				if !c.Quiet {
					log.Printf("CLIENT %v is done publishing\n", c.ID)
				}
				return
			}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now(), c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(onConnected).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
		log.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", c.ID, reason.Error())
	})
	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		log.Printf("CLIENT %v had error connecting to the broker: %v\n", c.ID, token.Error())
	}
}
