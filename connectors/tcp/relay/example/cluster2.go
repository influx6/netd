package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/influx6/netd"
	"github.com/influx6/netd/connectors/tcp"
	"github.com/influx6/netd/connectors/tcp/relay"
)

var (
	context = "example"
)

type logger struct {
	in io.Writer
}

func (l logger) Log(context interface{}, fn string, message string, data ...interface{}) {
	fmt.Fprintf(l.in, "Context[%s] :  Function[%s] :  %s\n", context, fn, fmt.Sprintf(message, data...))
}

func (l logger) Error(context interface{}, fn string, err error, message string, data ...interface{}) {
	fmt.Fprintf(l.in, "Context[%s] :  Function[%s] :  %s : Error[%s]\n", context, fn, fmt.Sprintf(message, data...), err)
}

type tracer struct {
	b  bytes.Buffer
	in io.Writer
}

func (t tracer) Begin(context interface{}, msg []byte) {
	t.b.Write([]byte(fmt.Sprintf("--TRACE [%+q] Begin------------------------------", msg)))
}

func (t tracer) Trace(context interface{}, msg []byte) {
	t.b.Write(msg)
}

func (t tracer) End(context interface{}, msg []byte) {
	t.b.Write([]byte(fmt.Sprintf("--TRACE [%+q] Ended------------------------------", msg)))
	t.in.Write(t.b.Bytes())
	t.b.Reset()
}

func main() {

	var c netd.Config

	c.Trace = tracer{in: os.Stdout}
	c.Log = logger{in: os.Stdout}

	c.Port = 5050
	c.Addr = "0.0.0.0"

	c.ClustersPort = 5055
	c.ClustersAddr = "0.0.0.0"

	tcp := tcp.New(c)

	if err := tcp.ServeClients(context, relay.ClientHandler); err != nil {
		panic(err)
	}

	if err := tcp.ServeClusters(context, relay.ClusterHandler); err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
