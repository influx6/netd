package tcp

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influx6/netd"
	"github.com/influx6/netd/routes"
	"github.com/pborman/uuid"
)

// Connections takes a net.Conn and returns a Connection instance which wraps the
// provided net.Conn for usage.
type Connections interface {
	netd.Connections
	netd.ClusterConnect

	NewClusterFrom(context interface{}, c net.Conn, initiator bool) error
}

// Connection defines a struct which stores the incoming request for a
// connection.
type Connection struct {
	net.Conn
	Connections

	Router     netd.Router
	Config     netd.Config
	ServerInfo netd.BaseInfo
	MyInfo     netd.BaseInfo

	Stat   netd.StatProvider
	Events netd.ConnectionEvents
}

// ClusterProvision defines a provision which returns a Provider for cluster
// connections.
type Handler func(context interface{}, c *Connection) (netd.Provider, error)

// TCPConn defines a baselevel connection wrapper which provides a flexibile
// tcp request management routine.
type TCPConn struct {
	netd.Stat

	mc             sync.Mutex
	runningClient  bool
	runningCluster bool
	sid            string
	clientAddr     string
	clusterAddr    string

	clientHandler  Handler
	clusterHandler Handler
	router         netd.Router

	closer        chan struct{}
	config        netd.Config
	tcpClient     net.Listener
	tcpCluster    net.Listener
	infoTCP       netd.BaseInfo
	infoCluster   netd.BaseInfo
	conWG         sync.WaitGroup
	opWG          sync.WaitGroup
	clients       []netd.Provider
	clusters      []netd.Provider
	clientEvents  netd.ConnectionEvents
	clusterEvents netd.ConnectionEvents
}

// New returns a new instance of connection provider.
func New(c netd.Config) *TCPConn {
	c.InitLogAndTrace()

	if err := c.ParseTLS(); err != nil {
		c.Error("netd.TCP", "TCP", err, "Error parsing tls arguments")
		panic(err)
	}

	sid := uuid.New()

	var cn TCPConn
	cn.sid = sid
	cn.config = c
	cn.router = routes.New(c.Trace)
	cn.clientEvents = netd.NewBaseEvent()
	cn.clusterEvents = netd.NewBaseEvent()

	return &cn
}

// Clients returns the list of available client connections.
func (c *TCPConn) Clients(context interface{}) netd.SearchableInfo {
	var infoList []netd.BaseInfo

	c.mc.Lock()
	for _, client := range c.clients {
		cb := client.BaseInfo()

		if c.infoTCP.Match(cb) {
			continue
		}

		infoList = append(infoList, cb)
	}
	c.mc.Unlock()

	return netd.SearchableInfo(infoList)
}

// Clusters returns a list of available clusters connections.
func (c *TCPConn) Clusters(context interface{}) netd.SearchableInfo {
	var infoList []netd.BaseInfo

	c.mc.Lock()
	for _, cluster := range c.clusters {
		cb := cluster.BaseInfo()

		if c.infoCluster.Match(cb) {
			continue
		}

		infoList = append(infoList, cb)
	}
	c.mc.Unlock()

	return netd.SearchableInfo(infoList)
}

// SendToClusters sends the provided message to all clusters.
func (c *TCPConn) SendToClusters(context interface{}, id string, msg []byte, flush bool) error {
	c.config.Log(context, "SendToCluster", "Started : Data[%+s]", msg)

	c.mc.Lock()
	defer c.mc.Unlock()

	if len(msg) >= netd.MAX_PAYLOAD_SIZE {
		return errors.New("Data size above max allowed")
	}

	for _, cluster := range c.clusters {
		if cluster.BaseInfo().ClientID == id {
			continue
		}

		var b [][]byte
		b = append(b, []byte("Trace: SendToClients"))
		b = append(b, netd.NewLine)
		b = append(b, []byte("Cluster: "))
		b = append(b, []byte(c.infoTCP.String()))
		b = append(b, netd.NewLine)
		b = append(b, []byte("ToCluster: "))
		b = append(b, []byte(cluster.BaseInfo().String()))
		b = append(b, netd.NewLine)
		b = append(b, []byte("Data: "))
		b = append(b, msg)
		b = append(b, netd.NewLine)

		c.config.Trace.Begin(context, []byte("SendToClusters"))
		c.config.Trace.Trace(context, bytes.Join(b, []byte("")))
		c.config.Trace.End(context, []byte("SendToClusters"))

		if err := cluster.SendMessage(context, msg, flush); err != nil {
			c.config.Error(context, "SendToClient", err, "Failed to deliver to cluster : Cluster[%s]", cluster.BaseInfo().String())
		}

	}

	c.config.Log(context, "SendToCluster", "Completed")
	return nil
}

// SendToClusters sends the provided message to all clients.
func (c *TCPConn) SendToClients(context interface{}, id string, msg []byte, flush bool) error {
	c.config.Log(context, "SendToClient", "Started : Data[%+s]", msg)

	c.mc.Lock()
	defer c.mc.Unlock()

	if len(msg) >= netd.MAX_PAYLOAD_SIZE {
		return errors.New("Data size above max allowed")
	}

	for _, client := range c.clients {
		if client.BaseInfo().ClientID == id {
			continue
		}

		var b [][]byte
		b = append(b, []byte("Trace: SendToClients"))
		b = append(b, netd.NewLine)
		b = append(b, []byte("Cluster: "))
		b = append(b, []byte(c.infoTCP.String()))
		b = append(b, netd.NewLine)
		b = append(b, []byte("ToClient: "))
		b = append(b, []byte(client.BaseInfo().String()))
		b = append(b, netd.NewLine)
		b = append(b, []byte("Data: "))
		b = append(b, msg)
		b = append(b, netd.NewLine)

		c.config.Trace.Begin(context, []byte("SendToClient"))
		c.config.Trace.Trace(context, bytes.Join(b, []byte("")))
		c.config.Trace.End(context, []byte("SendToClient"))

		if err := client.SendMessage(context, msg, flush); err != nil {
			c.config.Error(context, "SendToClient", err, "Failed to deliver to client : ClientInfo[%s]", client.BaseInfo().String())
		}

	}

	c.config.Log(context, "SendToClient", "Completed")
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
				c.config.Error(context, "Close", err, "Completed")
				c.mc.Unlock()
				return err
			}
		}

		if c.tcpCluster != nil {
			if err := c.tcpCluster.Close(); err != nil {
				c.config.Error(context, "Close", err, "Completed")
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
			c.config.Error(context, "Close", err, "Failed To Close Client")
		}
	}

	for _, cluster := range clusters {
		if err := cluster.Close("tcp.Close"); err != nil {
			c.config.Error(context, "Close", err, "Failed To Close Cluster")
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
func (c *TCPConn) ServeClusters(context interface{}, h Handler) error {
	c.config.Log(context, "tcp.ServeCluster", "Started : Initializing cluster service : ID[%s] : Addr[%s] : Port[%d]", c.sid, c.config.ClustersAddr, c.config.ClustersPort)
	addr := net.JoinHostPort(c.config.ClustersAddr, strconv.Itoa(c.config.ClustersPort))

	var err error
	c.mc.Lock()

	if c.runningCluster {
		c.config.Log(context, "tcp.ServeCluster", "Completed")
		c.mc.Unlock()
		return nil
	}

	c.clusterHandler = h

	c.tcpCluster, err = net.Listen("tcp", addr)
	if err != nil {
		c.config.Error(context, "tcp.ServeCluster", err, "Completed")
		c.mc.Unlock()
		return err
	}

	caddr := c.tcpCluster.Addr().String()
	c.clusterAddr = caddr

	ip, port, _ := net.SplitHostPort(caddr)
	iport, _ := strconv.Atoi(port)

	ip = strings.TrimSpace(ip)
	if ip == "" || ip == "::" {
		if c.config.ClustersAddr == "0.0.0.0" || c.config.ClustersAddr == "" {
			ip = "127.0.0.1"
		} else {
			ip = c.config.ClustersAddr

		}
	}

	var info netd.BaseInfo
	info.Addr = ip
	info.IP = ip
	info.RealAddr = ip
	info.LocalAddr = ip
	info.Port = iport
	info.RealPort = iport
	info.LocalPort = iport
	info.Version = netd.VERSION
	info.MaxPayload = netd.MAX_PAYLOAD_SIZE
	info.GoVersion = runtime.Version()
	info.ServerID = c.sid
	info.ClientID = uuid.New()
	info.ClusterNode = true

	c.runningCluster = true
	c.infoCluster = info

	c.mc.Unlock()

	go c.listenerLoop(context, true, c.tcpCluster, info, h)

	c.config.Log(context, "tcp.ServeCluster", "Completed")
	return nil
}

// ServeClients runs to create the listener for listening to client based
// requests for the tcp connection.
func (c *TCPConn) ServeClients(context interface{}, h Handler) error {
	c.config.Log(context, "tcp.ServeClients", "Started : Initializing clients service : ID[%s] : Addr[%s] : Port[%d]", c.sid, c.config.Addr, c.config.Port)
	addr := net.JoinHostPort(c.config.Addr, strconv.Itoa(c.config.Port))

	var err error
	c.mc.Lock()

	if c.runningClient {
		c.config.Log(context, "tcp.ServeClients", "Completed")
		c.mc.Unlock()
		return nil
	}

	c.clientHandler = h
	c.tcpClient, err = net.Listen("tcp", addr)
	if err != nil {
		c.config.Error(context, "tcp.ServeClients", err, "Completed")
		c.mc.Unlock()
		return err
	}

	caddr := c.tcpClient.Addr().String()
	c.clientAddr = caddr

	ip, port, _ := net.SplitHostPort(caddr)
	iport, _ := strconv.Atoi(port)

	ip = strings.TrimSpace(ip)
	if ip == "" || ip == "::" {
		if c.config.ClustersAddr == "0.0.0.0" || c.config.ClustersAddr == "" {
			ip = "127.0.0.1"
		} else {
			ip = c.config.ClustersAddr
		}
	}

	var info netd.BaseInfo
	info.Addr = ip
	info.IP = ip
	info.RealAddr = ip
	info.LocalAddr = ip
	info.Port = iport
	info.RealPort = iport
	info.LocalPort = iport
	info.Version = netd.VERSION
	info.MaxPayload = netd.MAX_PAYLOAD_SIZE
	info.GoVersion = runtime.Version()
	info.ServerID = c.sid
	c.infoTCP = info

	c.runningClient = true

	c.mc.Unlock()

	go c.listenerLoop(context, false, c.tcpClient, info, h)

	c.config.Log(context, "tcp.ServeClients", "Completed")
	return nil
}

//==============================================================================

// NewCluster creates a new connnection to a giving cluster addr and port, returning
// an error if the operation failed.
func (c *TCPConn) NewCluster(context interface{}, addr string, port int) error {
	c.config.Log(context, "tcp.NewCluster", "Started : New Cluster Request : Addr[%s] : Port[%d]", addr, port)

	c.mc.Lock()
	if !c.runningCluster {
		c.mc.Unlock()
		c.config.Error(context, "tcp.NewCluster", netd.ErrNoClusterService, "Completed")
		return netd.ErrNoClusterService
	}
	c.mc.Unlock()

	if addr == c.infoCluster.RealAddr && port == c.infoCluster.RealPort {
		err := errors.New("Incapable of connecting to self")
		c.config.Error(context, "tcp.NewCluster", err, "Completed")
		return netd.ErrSelfRequest
	}

	if (addr == "" || addr == "0.0.0.0") && port == c.infoCluster.RealPort {
		c.config.Error(context, "tcp.NewCluster", netd.ErrSelfRequest, "Completed")
		return netd.ErrSelfRequest
	}

	_, err := c.Clusters(context).HasAddr(addr, port)
	if err == nil {
		c.config.Error(context, "tcp.NewCluster", netd.ErrAlreadyConnected, "Completed")
		return netd.ErrAlreadyConnected
	}

	caddr := net.JoinHostPort(addr, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", caddr, netd.DEFAULT_DIAL_TIMEOUT)
	if err != nil {
		c.config.Error(context, "tcp.NewCluster", err, "Completed")
		return err
	}

	c.config.Log(context, "tcp.NewCluster", "Completed")
	return c.NewClusterFrom(context, conn, true)
}

func (c *TCPConn) NewClusterFrom(context interface{}, conn net.Conn, initiator bool) error {
	c.config.Log(context, "tcp.NewCusterFrom", "Started : For[%s]", conn.RemoteAddr().String())

	var info netd.BaseInfo

	c.mc.Lock()
	info = c.infoCluster
	c.mc.Unlock()

	info.ConnectInitiator = true
	info.HandleReconnect = true

	connection, err := c.newFromConn(context, conn, info)
	if err != nil {
		c.config.Error(context, "tcp.NewCusterFrom", err, "Completed")
		return err
	}

	connection.MyInfo.ConnectInitiator = true
	connection.MyInfo.ClusterNode = true
	connection.MyInfo.HandleReconnect = true

	c.config.Log(context, "tcp.NewClusterFrom", "Created net.Conn : Info[%#v]", connection.MyInfo)

	if err := c.newClusterConn(context, connection); err != nil {
		c.config.Error(context, "tcp.NewCusterFrom", err, "Completed")
		return err
	}

	c.config.Log(context, "tcp.NewCusterFrom", "Completed")
	return nil
}

//==============================================================================

func (c *TCPConn) listenerLoop(context interface{}, isCluster bool, listener net.Listener, info netd.BaseInfo, h Handler) {
	c.config.Log(context, "tcp.listenerLoop", "Started")

	c.mc.Lock()
	config := c.config
	c.mc.Unlock()

	c.mc.Lock()
	c.opWG.Add(1)
	defer c.opWG.Done()
	c.mc.Unlock()

	sleepTime := netd.ACCEPT_MIN_SLEEP

	{
		for c.IsRunning() {

			clusterSize := len(c.clusters)
			clientSize := len(c.clients)

			conn, err := listener.Accept()
			if err != nil {
				config.Error(context, "tcp.listenerLoop", err, "Accept Error")
				if tmpError, ok := err.(net.Error); ok && tmpError.Temporary() {
					config.Log(context, "tcp.listenerLoop", "Temporary error recieved, sleeping for %dms", sleepTime/time.Millisecond)
					time.Sleep(sleepTime)
					sleepTime *= 2
					if sleepTime > netd.ACCEPT_MAX_SLEEP {
						sleepTime = netd.ACCEPT_MIN_SLEEP
					}
				}

				continue
			}

			config.Log(context, "tcp.listenerLoop", "New Connection : Addr[%s]  : Local[%s] : New Connection", conn.RemoteAddr().String(), conn.LocalAddr().String())
			connection, err := c.newFromConn(context, conn, info)
			if err != nil {
				config.Error(context, "tcp.listenerLoop", err, "New Connection : Addr[%s] : Failed Create *Connection", conn.RemoteAddr().String())
				continue
			}

			if isCluster {
				if clusterSize >= netd.DEFAULT_MAX_CONNECTIONS {
					config.Error(context, "tcp.listenerLoop", err, "Max Cluster Connection Reached")
					conn.Close()
					continue
				}

				if err := c.newClusterConn(context, connection); err != nil {
					config.Error(context, "tcp.listenerLoop", err, "New Connection : Addr[%s] : Failed Create *Connection", conn.RemoteAddr().String())
				}

				continue
			}

			if clientSize >= netd.DEFAULT_MAX_CONNECTIONS {
				config.Error(context, "tcp.listenerLoop", err, "Max Client Connection Reached")
				conn.Close()
				continue
			}

			if err := c.newClientConn(context, connection); err != nil {
				config.Error(context, "tcp.listenerLoop", err, "New Connection : Addr[%s] : Failed Create *Connection", conn.RemoteAddr().String())
				continue
			}

		}
	}

	c.config.Log(context, "tcp.listenerLoop", "Completed")
}

func (c *TCPConn) newClusterConn(context interface{}, connection *Connection) error {
	config := connection.Config

	clusters := c.Clusters(context)
	if clusters.HasInfo(connection.MyInfo) {
		config.Error(context, "tcp.newClusterConn", netd.ErrAlreadyConnected, "New Connection : Addr[%a] : Failed Provider Creation", connection.RemoteAddr().String())
		return netd.ErrAlreadyConnected
	}

	config.Log(context, "tcp.newClusterConn", "Creating Provider for Addr[%+s] ", connection.RemoteAddr().String())

	provider, err := c.clusterHandler(context, connection)
	if err != nil {
		config.Error(context, "tcp.newClusterConn", err, "New Connection : Addr[%a] : Failed Provider Creation", connection.RemoteAddr().String())
		connection.SetReadDeadline(time.Time{})
		connection.Close()
	}
	config.Log(context, "tcp.newClusterConn", "Provider Created for Addr[%+s] ", connection.RemoteAddr().String())

	config.Log(context, "tcp.newClusterConn", "Provider Authentication Process Initiated for Addr[%+s] ", connection.RemoteAddr().String())

	// Check authentication of provider and certify if we are authorized.
	if config.Authenticate {
		config.Log(context, "tcp.newClusterConn", "Provider Authentication Process Started for Addr[%+s] ", connection.RemoteAddr().String())

		providerAuth, ok := provider.(netd.ClientAuth)

		if !ok && c.config.MustAuthenticate {
			config.Error(context, "tcp.newClusterConn", err, "New Connection : Addr[%a] : Provider does not match ClientAuth interface", connection.RemoteAddr().String())
			provider.SendError(context, true, errors.New("Error: Provider has no authentication. Authentication needed"))
			provider.Close(context)
			return errors.New("Provider has no authenticator")
		}

		if !config.ClusterAuth.Authenticate(providerAuth) {
			if !config.MatchClusterCredentials(providerAuth.Credentials()) {
				config.Error(context, "tcp.newClusterConn", err, "New Connection : Addr[%a] : Provider does not match ClientAuth interface", connection.RemoteAddr().String())
				provider.SendError(context, true, errors.New("Error: Authentication failed"))
				provider.Close(context)
				return errors.New("Authentication failed")
			}
		}
	} else {
		config.Log(context, "tcp.newClusterConn", "Provider Needs No Authentication for Addr[%+s] ", connection.RemoteAddr().String())
	}

	raddr := connection.RemoteAddr().String()

	// Listen for the end signal and descrease connection wait group.
	go func() {
		<-provider.CloseNotify()
		config.Log(context, "tcp.newClusterConn", "Provider with Addr[%+s] ending connection ", raddr)

		c.mc.Lock()
		{
			c.conWG.Done()
			clen := len(c.clusters)

			for index, pr := range c.clusters {
				if pr != provider {
					continue
				}

				if index+1 < clen {
					c.clusters = append(c.clusters[:index], c.clusters[index+1:]...)
				} else {
					c.clusters = c.clusters[:clen-1]
				}

				break
			}
		}
		c.mc.Unlock()

		c.clusterEvents.FireDisconnect(provider)

		{
			myInfo := connection.MyInfo
			serverInfo := connection.ServerInfo
			var reCount int
			reWait := netd.DEFAULT_RECONNECT_INTERVAL

		reconnectReduce:
			{
				if !myInfo.ExitedNormaly && myInfo.ClusterNode && serverInfo.ConnectInitiator && serverInfo.HandleReconnect {
					if err := c.reconnectHandler(context, myInfo); err != nil {
						c.config.Error(context, "newClusterConn", err, "Failed Reconnection : Total Time[%s] : Total Retries[%d] : Info[%s]", reWait, reCount, myInfo.ID())

						reCount++
						if reCount >= netd.MAX_RECONNECT_COUNT {
							return
						}

						reWait = reWait + netd.DEFAULT_RECONNECT_INTERVAL
						<-time.After(reWait)
						goto reconnectReduce
					}
				}
			}
		}
	}()

	c.mc.Lock()
	{
		c.conWG.Add(1)
		c.clusters = append(c.clusters, provider)
	}
	c.mc.Unlock()

	config.Log(context, "tcp.newClusterConn", "Provider Ready Addr[%+s] ", connection.RemoteAddr().String())
	c.clusterEvents.FireConnect(provider)
	return nil
}

func (c *TCPConn) newClientConn(context interface{}, connection *Connection) error {
	config := connection.Config

	clients := c.Clients(context)
	if clients.HasInfo(connection.MyInfo) {
		config.Error(context, "tcp.newClientConn", netd.ErrAlreadyConnected, "New Connection : Addr[%a] : Failed Provider Creation", connection.RemoteAddr().String())
		return netd.ErrAlreadyConnected
	}

	config.Log(context, "tcp.newClientConn", "Creating Provider for Addr[%+s] ", connection.RemoteAddr().String())

	provider, err := c.clientHandler(context, connection)
	if err != nil {
		config.Error(context, "tcp.newClientConn", err, "New Connection : Addr[%a] : Failed Provider Creation", connection.RemoteAddr().String())
		connection.SetReadDeadline(time.Time{})
		connection.Close()
	}

	config.Log(context, "tcp.newClientConn", "Provider Created for Addr[%+s] ", connection.RemoteAddr().String())
	config.Log(context, "tcp.newClientConn", "Provider Authentication Process Initiated for Addr[%+s] ", connection.RemoteAddr().String())

	// Check authentication of provider and certify if we are authorized.
	if config.Authenticate {
		config.Log(context, "tcp.newClientConn", "Provider Authentication Process Started for Addr[%+s] ", connection.RemoteAddr().String())
		providerAuth, ok := provider.(netd.ClientAuth)
		if !ok && c.config.MustAuthenticate {
			config.Error(context, "tcp.newClientConn", err, "New Connection : Addr[%a] : Provider does not match ClientAuth interface", connection.RemoteAddr().String())
			provider.SendError(context, true, errors.New("Error: Provider has no authentication. Authentication needed"))
			provider.Close(context)
			return errors.New("Provider has no authenticator")
		}

		if !config.ClientAuth.Authenticate(providerAuth) {
			if !config.MatchClientCredentials(providerAuth.Credentials()) {
				config.Error(context, "tcp.newClientConn", err, "New Connection : Addr[%a] : Provider does not match ClientAuth interface", connection.RemoteAddr().String())
				provider.SendError(context, true, errors.New("Error: Authentication failed"))
				provider.Close(context)
				return errors.New("Authentication failed")
			}

		}
	} else {
		config.Log(context, "tcp.newClientConn", "Provider Needs No Authentication for Addr[%+s] ", connection.RemoteAddr().String())
	}

	// Listen for the end signal and descrease connection wait group.
	raddr := connection.RemoteAddr().String()
	go func() {
		<-provider.CloseNotify()
		config.Log(context, "tcp.newClientConn", "Provider with Addr[%+s] ending connection ", raddr)

		c.mc.Lock()
		{
			c.conWG.Done()
			clen := len(c.clients)

			for index, pr := range c.clients {
				if pr != provider {
					continue
				}

				if index+1 < clen {
					c.clients = append(c.clients[:index], c.clients[index+1:]...)
				} else {
					c.clients = c.clients[:clen-1]
				}

				break
			}
		}
		c.mc.Unlock()

		c.clientEvents.FireDisconnect(provider)
	}()

	c.mc.Lock()
	{
		c.conWG.Add(1)
		c.clients = append(c.clients, provider)
	}
	c.mc.Unlock()

	config.Log(context, "tcp.newClientConn", "Provider Ready Addr[%+s] ", connection.RemoteAddr().String())
	c.clientEvents.FireConnect(provider)
	return nil
}

func (c *TCPConn) reconnectHandler(context interface{}, info netd.BaseInfo) error {
	c.config.Log(context, "reconnectHandler", "Retrying Connection : Info[%s]", info.ID())

	if err := c.NewCluster(context, info.Addr, info.Port); err != nil {
		c.config.Error(context, "reconnectHandler", err, "Completed")
		return err
	}

	c.config.Log(context, "reconnectHandler", "Completed")
	return nil
}

func (c *TCPConn) newFromConn(context interface{}, conn net.Conn, serverInfo netd.BaseInfo) (*Connection, error) {
	c.config.Log(context, "newFromConn", "New Connection : Addr[%s]", conn.RemoteAddr().String())

	var stat netd.StatProvider

	c.mc.Lock()
	stat = c.Stat
	config := c.config
	useTLS := c.config.UseTLS
	c.mc.Unlock()

	addr, port, _ := net.SplitHostPort(conn.RemoteAddr().String())
	laddr, lport, _ := net.SplitHostPort(conn.LocalAddr().String())

	iport, _ := strconv.Atoi(port)
	liport, _ := strconv.Atoi(lport)

	var connInfo netd.BaseInfo
	connInfo.Addr = addr
	connInfo.Port = iport
	connInfo.RealAddr = addr
	connInfo.RealPort = iport
	connInfo.LocalAddr = laddr
	connInfo.LocalPort = liport

	connInfo.ClientID = uuid.New()
	connInfo.GoVersion = serverInfo.GoVersion
	connInfo.MaxPayload = serverInfo.MaxPayload
	connInfo.ServerID = serverInfo.ServerID
	connInfo.Version = netd.VERSION

	config.Log(context, "tcp.newFromConn", "New Connection : Server[%#v] : Client[%#v]", serverInfo, connInfo)

	var connection Connection
	connection.ServerInfo = serverInfo
	connection.MyInfo = connInfo
	connection.Config = config
	connection.Connections = c
	connection.Stat = stat
	connection.Router = c.router

	// Check if we are required to be using TLS then try to wrap net.Conn
	// to tls.Conn.
	if useTLS {
		tlsConn := tls.Server(conn, config.TLSConfig)
		ttl := secondsToDuration(netd.TLS_TIMEOUT * float64(time.Second))

		var tlsPassed bool

		time.AfterFunc(ttl, func() {
			config.Log(context, "newFromConn", "Connection TLS Handshake Timeout : Status[%s] : Addr[%a]", tlsPassed, conn.RemoteAddr().String())

			// Once the time has elapsed, close the connection and nil out.
			if !tlsPassed {
				tlsConn.SetReadDeadline(time.Time{})
				tlsConn.Close()
			}
		})

		tlsConn.SetReadDeadline(time.Now().Add(ttl))

		if err := tlsConn.Handshake(); err != nil {
			config.Error(context, "newFromConn", err, "New Connection : Addr[%s] : Failed Handshake", conn.RemoteAddr().String())
			tlsConn.SetReadDeadline(time.Time{})
			tlsConn.Close()
			return nil, err
		}

		connection.Conn = tlsConn
	} else {
		connection.Conn = conn
	}

	return &connection, nil
}

func secondsToDuration(seconds float64) time.Duration {
	ttl := seconds * float64(time.Second)
	return time.Duration(ttl)
}
