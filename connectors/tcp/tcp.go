package tcp

import (
	"bytes"
	"crypto/tls"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/influx6/netd"
	"github.com/influx6/netd/routes"
	"github.com/pborman/uuid"
)

var (
	ctrl = `\r\n`

	emptyString = []byte("")
	endTrace    = []byte("End Trace")
	ctrlLine    = []byte(ctrl)
	newLine     = []byte("\n")
)

// TCPConn defines a baselevel connection wrapper which provides a flexibile
// tcp request management routine.
type TCPConn struct {
	netd.Stat

	mc             sync.Mutex
	runningClient  bool
	runningCluster bool
	sid            string
	config         netd.Config
	infoTCP        netd.BaseInfo
	infoCluster    netd.BaseInfo
	tcpClient      net.Listener
	tcpCluster     net.Listener
	clients        []netd.Provider
	clusters       []netd.Provider
	closer         chan struct{}
	router         *routes.Subscription
	conWG          sync.WaitGroup
	opWG           sync.WaitGroup
	clientEvents   netd.ConnectionEvents
	clusterEvents  netd.ConnectionEvents
}

// New returns a new instance of connection provider.
func New(c netd.Config) *TCPConn {
	c.InitLogAndTrace()

	if err := c.ParseTLS(); err != nil {
		c.Log.Error("netd.TCP", "TCP", err, "Error parsing tls arguments")
		panic(err)
	}

	sid := uuid.New()

	var info netd.BaseInfo
	info.Addr = c.Addr
	info.Port = c.Port
	info.Version = netd.VERSION
	info.GoVersion = runtime.Version()
	info.ServerID = sid

	var cinfo netd.BaseInfo
	cinfo.Addr = c.ClustersAddr
	cinfo.Port = c.ClustersPort
	cinfo.Version = netd.VERSION
	cinfo.GoVersion = runtime.Version()
	cinfo.ServerID = sid

	var cn TCPConn
	cn.sid = sid
	cn.config = c
	cn.infoTCP = info
	cn.infoCluster = cinfo
	cn.clientEvents = netd.NewBaseEvent()
	cn.clusterEvents = netd.NewBaseEvent()

	return &cn
}

// Clients returns the list of available client connections.
func (c *TCPConn) Clients(context interface{}) netd.SearchableInfo {
	var infoList []netd.BaseInfo

	c.mc.Lock()
	for _, client := range c.clients {
		infoList = append(infoList, client.BaseInfo())
	}
	c.mc.Unlock()

	return netd.SearchableInfo(infoList)
}

// Clusters returns a list of available clusters connections.
func (c *TCPConn) Clusters(context interface{}) netd.SearchableInfo {
	var infoList []netd.BaseInfo

	c.mc.Lock()
	for _, cluster := range c.clusters {
		infoList = append(infoList, cluster.BaseInfo())
	}
	c.mc.Unlock()

	return netd.SearchableInfo(infoList)
}

// SendToClusters sends the provided message to all clusters.
func (c *TCPConn) SendToClusters(context interface{}, msg []byte, flush bool) error {
	c.config.Log.Log(context, "SendToCluster", "Started : Data[%+s]", msg)

	c.mc.Lock()
	defer c.mc.Unlock()

	for _, cluster := range c.clusters {

		var b [][]byte
		b = append(b, []byte("Trace: SendToClients"))
		b = append(b, newLine)
		b = append(b, []byte("Cluster: "))
		b = append(b, []byte(c.infoTCP.String()))
		b = append(b, newLine)
		b = append(b, []byte("ToCluster: "))
		b = append(b, []byte(cluster.BaseInfo().String()))
		b = append(b, newLine)
		b = append(b, []byte("Data: "))
		b = append(b, msg)
		b = append(b, newLine)
		c.config.Trace.Trace(context, bytes.Join(b, emptyString))

		if err := cluster.SendMessage(context, msg, flush); err != nil {
			c.config.Log.Error(context, "SendToCluster", err, "Failed to deliver to cluster : Cluster[%s]", cluster.BaseInfo().String())
		}

		c.config.Trace.Trace(context, endTrace)
	}

	c.config.Log.Log(context, "SendToCluster", "Completed")
	return nil
}

// SendToClusters sends the provided message to all clients.
func (c *TCPConn) SendToClients(context interface{}, msg []byte, flush bool) error {
	c.config.Log.Log(context, "SendToClient", "Started : Data[%+s]", msg)

	c.mc.Lock()
	defer c.mc.Unlock()

	for _, client := range c.clients {

		var b [][]byte
		b = append(b, []byte("Trace: SendToClients"))
		b = append(b, newLine)
		b = append(b, []byte("Cluster: "))
		b = append(b, []byte(c.infoTCP.String()))
		b = append(b, newLine)
		b = append(b, []byte("ToClient: "))
		b = append(b, []byte(client.BaseInfo().String()))
		b = append(b, newLine)
		b = append(b, []byte("Data: "))
		b = append(b, msg)
		b = append(b, newLine)
		c.config.Trace.Trace(context, bytes.Join(b, emptyString))

		if err := client.SendMessage(context, msg, flush); err != nil {
			c.config.Log.Error(context, "SendToClient", err, "Failed to deliver to client : ClientInfo[%s]", client.BaseInfo().String())
		}

		c.config.Trace.Trace(context, endTrace)
	}

	c.config.Log.Log(context, "SendToClient", "Completed")
	return nil
}

// Close ends the tcp connection handler and its internal clusters and clients.
func (c *TCPConn) Close(context interface{}) error {
	if !c.IsRunning() {
		return nil
	}

	c.mc.Lock()
	c.runningClient = false
	c.runningCluster = false
	c.mc.Unlock()

	c.opWG.Wait()

	c.mc.Lock()
	{
		if c.tcpClient != nil {
			if err := c.tcpClient.Close(); err != nil {
				c.config.Log.Error(context, "Close", err, "Completed")
				c.mc.Unlock()
				return err
			}
		}

		if c.tcpCluster != nil {
			if err := c.tcpCluster.Close(); err != nil {
				c.config.Log.Error(context, "Close", err, "Completed")
				c.mc.Unlock()
				return err
			}
		}
	}
	c.mc.Unlock()

	var clients, clusters []netd.Provider

	c.mc.Lock()
	{
		clients = append([]netd.Provider{}, c.clients...)
		clusters = append([]netd.Provider{}, c.clusters...)
	}
	c.mc.Unlock()

	for _, client := range clients {
		if err := client.Close("tcp.Close"); err != nil {
			c.config.Log.Error(context, "Close", err, "Failed To Close Client")
		}
	}

	for _, cluster := range clusters {
		if err := cluster.Close("tcp.Close"); err != nil {
			c.config.Log.Error(context, "Close", err, "Failed To Close Cluster")
		}
	}

	c.mc.Lock()
	{
		close(c.closer)
	}
	c.mc.Unlock()

	return nil
}

// IsRunning returns true/false if the connection is up.
func (c *TCPConn) IsRunning() bool {
	var state bool
	c.mc.Lock()
	state = c.runningClient || c.runningCluster
	c.mc.Unlock()
	return state
}

// ServeClusters runs to create the listener for listening to cluster based
// requests for the tcp connection.
func (c *TCPConn) ServeClusters(context interface{}, h netd.Handler) error {
	c.config.Log.Log(context, "tcp.ServeCluster", "Started : Initializing cluster service : Addr[%s] : Port[%d]", c.config.ClustersAddr, c.config.ClustersPort)
	addr := net.JoinHostPort(c.config.ClustersAddr, strconv.Itoa(c.config.ClustersPort))

	var err error
	c.mc.Lock()

	if c.runningCluster {
		c.config.Log.Log(context, "tcp.ServeCluster", "Completed")
		c.mc.Unlock()
		return nil
	}

	c.tcpCluster, err = net.Listen("tcp", addr)
	if err != nil {
		c.config.Log.Error(context, "tcp.ServeCluster", err, "Completed")
		c.mc.Unlock()
		return err
	}

	ip, port, _ := net.SplitHostPort(c.tcpCluster.Addr().String())
	iport, _ := strconv.Atoi(port)

	var info netd.BaseInfo
	info.IP = ip
	info.Port = iport
	info.Version = netd.VERSION
	info.MaxPayload = netd.MAX_PAYLOAD_SIZE
	info.GoVersion = runtime.Version()
	info.ServerID = c.sid

	c.runningCluster = true

	c.mc.Unlock()

	go c.listenerLoop(context, true, c.tcpCluster, h, info)

	c.config.Log.Log(context, "tcp.ServeCluster", "Completed")
	return nil
}

// ServeClients runs to create the listener for listening to client based
// requests for the tcp connection.
func (c *TCPConn) ServeClients(context interface{}, h netd.Handler) error {
	c.config.Log.Log(context, "tcp.ServeClients", "Started : Initializing client service : Addr[%s] : Port[%d]", c.config.Addr, c.config.Port)
	addr := net.JoinHostPort(c.config.Addr, strconv.Itoa(c.config.Port))

	var err error
	c.mc.Lock()

	if c.runningClient {
		c.config.Log.Log(context, "tcp.ServeClients", "Completed")
		c.mc.Unlock()
		return nil
	}

	c.tcpClient, err = net.Listen("tcp", addr)
	if err != nil {
		c.config.Log.Error(context, "tcp.ServeClients", err, "Completed")
		c.mc.Unlock()
		return err
	}

	ip, port, _ := net.SplitHostPort(c.tcpClient.Addr().String())
	iport, _ := strconv.Atoi(port)

	var info netd.BaseInfo
	info.IP = ip
	info.Port = iport
	info.Version = netd.VERSION
	info.MaxPayload = netd.MAX_PAYLOAD_SIZE
	info.GoVersion = runtime.Version()
	info.ServerID = c.sid

	c.runningClient = true

	c.mc.Unlock()

	go c.listenerLoop(context, false, c.tcpClient, h, info)

	c.config.Log.Log(context, "tcp.ServeClients", "Completed")
	return nil
}

func (c *TCPConn) listenerLoop(context interface{}, isCluster bool, listener net.Listener, h netd.Handler, info netd.BaseInfo) {
	c.config.Log.Log(context, "tcp.listenerLoop", "Started")

	var stat netd.StatProvider

	c.mc.Lock()
	stat = c.Stat
	config := c.config
	useTLS := c.config.UseTLS
	c.mc.Unlock()

	c.mc.Lock()
	c.opWG.Add(1)
	defer c.opWG.Done()
	c.mc.Unlock()

	sleepTime := netd.ACCEPT_MIN_SLEEP

	{
		for c.IsRunning() {

			conn, err := listener.Accept()
			if err != nil {
				config.Log.Error(context, "tcp.listenerLoop", err, "Accept Error")
				if tmpError, ok := err.(net.Error); ok && tmpError.Temporary() {
					config.Log.Log(context, "tcp.listenerLoop", "Temporary error recieved, sleeping for %dms", sleepTime/time.Millisecond)
					time.Sleep(sleepTime)
					sleepTime *= 2
					if sleepTime > netd.ACCEPT_MAX_SLEEP {
						sleepTime = netd.ACCEPT_MIN_SLEEP
					}
				}

				continue
			}

			sleepTime = netd.ACCEPT_MIN_SLEEP
			config.Log.Log(context, "tcp.listenerLoop", "New Connection : Addr[%a]", conn.RemoteAddr().String())

			var connection netd.Connection

			addr, port, _ := net.SplitHostPort(conn.RemoteAddr().String())
			iport, _ := strconv.Atoi(port)

			var connInfo netd.BaseInfo
			connInfo.Addr = addr
			connInfo.Port = iport
			connInfo.GoVersion = runtime.Version()
			connInfo.MaxPayload = netd.MAX_PAYLOAD_SIZE
			connInfo.ServerID = uuid.New()
			connInfo.Version = netd.VERSION

			// Check if we are required to be using TLS then try to wrap net.Conn
			// to tls.Conn.
			if useTLS {

				tlsConn := tls.Server(conn, config.TLSConfig)
				ttl := secondsToDuration(netd.TLS_TIMEOUT * float64(time.Second))

				var tlsPassed bool

				time.AfterFunc(ttl, func() {
					config.Log.Log(context, "tcp.listenerLoop", "Connection TLS Handshake Timeout : Status[%s] : Addr[%a]", tlsPassed, conn.RemoteAddr().String())

					// Once the time has elapsed, close the connection and nil out.
					if !tlsPassed {
						tlsConn.SetReadDeadline(time.Time{})
						tlsConn.Close()
					}
				})

				tlsConn.SetReadDeadline(time.Now().Add(ttl))

				if err := tlsConn.Handshake(); err != nil {
					config.Log.Error(context, "tcp.listenerLoop", err, "New Connection : Addr[%a] : Failed Handshake", conn.RemoteAddr().String())
					tlsConn.SetReadDeadline(time.Time{})
					tlsConn.Close()
					continue
				}

				connection = netd.Connection{
					Conn:           tlsConn,
					Router:         c.router,
					Config:         config,
					ServerInfo:     info,
					ConnectionInfo: connInfo,
					Events:         c.clientEvents,
					BroadCaster:    c,
					Connections:    c,
					Stat:           stat,
				}

			} else {

				connection = netd.Connection{
					Conn:           conn,
					Router:         c.router,
					Config:         config,
					ServerInfo:     info,
					ConnectionInfo: connInfo,
					Events:         c.clientEvents,
					BroadCaster:    c,
					Connections:    c,
					Stat:           stat,
				}

			}

			config.Log.Log(context, "tcp.listenerLoop", "Creating Provider for Addr[%+s] ", conn.RemoteAddr().String())
			provider, err := h(context, &connection)
			if err != nil {
				config.Log.Error(context, "tcp.listenerLoop", err, "New Connection : Addr[%a] : Failed Provider Creation", conn.RemoteAddr().String())
				connection.SetReadDeadline(time.Time{})
				connection.Close()
			}
			config.Log.Log(context, "tcp.listenerLoop", "Provider Created for Addr[%+s] ", conn.RemoteAddr().String())

			config.Log.Log(context, "tcp.listenerLoop", "Provider Authentication Process Initiated for Addr[%+s] ", conn.RemoteAddr().String())

			// Check authentication of provider and certify if we are authorized.
			if config.Authenticate {
				config.Log.Log(context, "tcp.listenerLoop", "Provider Authentication Process Started for Addr[%+s] ", conn.RemoteAddr().String())
				providerAuth, ok := provider.(netd.ClientAuth)
				if !ok && c.config.MustAuthenticate {
					config.Log.Error(context, "tcp.listenerLoop", err, "New Connection : Addr[%a] : Provider does not match ClientAuth interface", conn.RemoteAddr().String())
					provider.SendMessage(context, []byte("Error: Provider has no authentication. Authentication needed"), true)
					provider.Close(context)
					continue
				}

				if !config.ClientAuth.Authenticate(providerAuth) {
					if config.MatchClientCredentials(providerAuth.Credentials()) {
						c.mc.Lock()
						c.clients = append(c.clients, provider)
						c.mc.Unlock()
						continue
					}

					config.Log.Error(context, "tcp.listenerLoop", err, "New Connection : Addr[%a] : Provider does not match ClientAuth interface", conn.RemoteAddr().String())
					provider.SendMessage(context, []byte("Error: Authentication failed"), true)
					provider.Close(context)
					continue
				}
			} else {
				config.Log.Log(context, "tcp.listenerLoop", "Provider Needs No Authentication for Addr[%+s] ", conn.RemoteAddr().String())
			}

			raddr := conn.RemoteAddr().String()
			// Listen for the end signal and descrease connection wait group.
			go func() {
				<-provider.CloseNotify()
				config.Log.Log(context, "tcp.listenerLoop", "Provider with Addr[%+s] ending connection ", raddr)
				c.conWG.Done()
				if isCluster {
					c.clusterEvents.FireDisconnect(provider)
				} else {
					c.clientEvents.FireDisconnect(provider)
				}
			}()

			c.mc.Lock()
			{
				c.conWG.Add(1)
				if isCluster {
					c.clusters = append(c.clusters, provider)
				} else {
					c.clients = append(c.clients, provider)
				}
			}
			c.mc.Unlock()

			config.Log.Log(context, "tcp.listenerLoop", "Provider Ready Addr[%+s] ", conn.RemoteAddr().String())
			if isCluster {
				c.clusterEvents.FireConnect(provider)
			} else {
				c.clientEvents.FireConnect(provider)
			}

			continue
		}
	}

	c.config.Log.Log(context, "tcp.listenerLoop", "Completed")
}

func secondsToDuration(seconds float64) time.Duration {
	ttl := seconds * float64(time.Second)
	return time.Duration(ttl)
}
