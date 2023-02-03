// This is an NSQ client that reads the specified topic/channel
// and re-publishes as Http long-polling

package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/nsqio/go-nsq"
)

var (
	address = flag.String("address", ":4152", "http server address")

	nsqdTCPAddr     = flag.String("nsqd-tcp-address", ":4150", "nsqd TCP address")
	lookupdHTTPAddr = flag.String("lookupd-http-address", ":4161", "lookupd HTTP address")
)

// A pump, consume nsq.Message and publish as Http Response
type Pump struct {
	ch   chan *nsq.Message
	done chan bool // this nsq.Message has send to http socket
	nc   *nsq.Consumer
}

func New() *Pump {
	return &Pump{
		ch:   make(chan *nsq.Message, 1),
		done: make(chan bool, 1),
	}
}

// start nsq client consumer
func (p *Pump) Start(topic, channel string) error {
	c, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
	if err != nil {
		return err
	}

	c.AddHandler(p)

	if *nsqdTCPAddr != "" {
		e := c.ConnectToNSQD(*nsqdTCPAddr)
		if e != nil {
			return e
		}
	} else if *lookupdHTTPAddr != "" {
		e := c.ConnectToNSQLookupd(*lookupdHTTPAddr)
		if e != nil {
			return e
		}
	}
	p.nc = c
	return nil
}

// Handle nsq message
func (p *Pump) HandleMessage(message *nsq.Message) error {
	p.ch <- message
	flag := <-p.done

	if !flag {
		return errors.New("not done")
	}
	return nil
}

func (p *Pump) Handle(w http.ResponseWriter, r *http.Request) {
	f, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(400)
		return
	}

	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	topic_name := reqParams.Get("topic")
	if topic_name == "" {
		w.WriteHeader(400)
		return
	}

	channel_name := reqParams.Get("channel")
	if channel_name == "" {
		w.WriteHeader(400)
		return
	}

	ctx := r.Context()
	go func() {
		<-ctx.Done()
		if ctx.Err() != nil {
			close(p.ch) // better than p.ch <- nil
		}
	}()

	if err := p.Start(topic_name, channel_name); err != nil {
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
	m := <-p.ch

	if m == nil {
		log.Printf("client closed")
		p.done <- false
		p.nc.Stop()
		return
	}

	w.Write(m.Body)
	f.Flush()

	p.nc.Stop()

	p.done <- true
}

func main() {
	flag.Parse()

	//
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	// http server, use default mux
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		pump := New()
		pump.Handle(w, r)
	})
	log.Fatal(http.ListenAndServe(*address, nil))
}
