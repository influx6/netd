package main

import (
	"os"
	"os/signal"

	"github.com/influx6/netd"
	"github.com/influx6/netd/mocks"
	"github.com/influx6/netd/nitro"
	"github.com/influx6/netd/tcp"
)

var (
	context = "example"
)

func main() {

	var c netd.Config
	c.Trace = mocks.NewTracer(os.Stdout)
	c.Logger = mocks.NewLogger(os.Stdout)

	c.Port = 5050
	c.Addr = "0.0.0.0"

	c.ClustersPort = 5055
	c.ClustersAddr = "0.0.0.0"

	tcp := tcp.New(c)

	if err := tcp.ServeClients(context, nitro.Handlers.NewTCPClient); err != nil {
		panic(err)
	}

	if err := tcp.ServeClusters(context, nitro.Handlers.NewTCPCluster); err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
