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

type Foo struct {
	ch   chan *nsq.Message
	done chan bool
	w    http.ResponseWriter
	r    *http.Request
	c    *nsq.Consumer
}

func New(w http.ResponseWriter, r *http.Request) *Foo {
	return &Foo{
		ch:   make(chan *nsq.Message, 1),
		done: make(chan bool, 1),
		w:    w,
		r:    r,
	}
}

func (foo *Foo) Start(topic, channel string) error {
	c, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
	if err != nil {
		return err
	}

	c.AddHandler(foo)

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
	foo.c = c
	return nil
}

func (foo *Foo) HandleMessage(message *nsq.Message) error {
	foo.ch <- message
	flag := <-foo.done

	if !flag {
		return errors.New("not done")
	}
	return nil
}

func (foo *Foo) Handle(w http.ResponseWriter, r *http.Request) {
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

	if err := foo.Start(topic_name, channel_name); err != nil {
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
	m := <-foo.ch

	w.Write(m.Body)
	f.Flush()

	foo.c.Stop()

	foo.done <- true
}

func main() {
	flag.Parse()

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f := New(w, r)
		f.Handle(w, r)
	})
	log.Fatal(http.ListenAndServe(*address, nil))
}
