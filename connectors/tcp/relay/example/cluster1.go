package main

import (
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
	in io.Writer
}

func (t tracer) Trace(context interface{}, msg []byte) {
	fmt.Fprintf(t.in, "%s", msg)
}

func main() {

	var c netd.Config

	c.Trace = tracer{in: os.Stdout}
	c.Log = logger{in: os.Stdout}

	c.Port = 4050
	c.Addr = "0.0.0.0"

	c.ClustersPort = 4055
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
