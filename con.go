package netd

import "sync"

// Messages defines an interface which exposes methods for sending messages down
// a connection pipeline.
type Messages interface {
	SendMessage(context interface{}, msg []byte, flush bool) error
	SendError(context interface{}, flush bool, msg ...error) error
	SendRequest(context interface{}, flush bool, msg ...[][]byte) error
	SendResponse(context interface{}, flush bool, msg ...[][]byte) error
}

// RequestResponse defines an interface for a provider which handles the
// processinging of requests and its response to a provider.
type RequestResponse interface {
	Process(context interface{}, msg []byte, m Messages) error
}

// Provider defines a interface for a connection handler, which ensures
// to manage the request-response cycle of a provided net.Conn.
type Provider interface {
	Messages
	Subscriber
	BaseInfo() BaseInfo
	CloseNotify() chan struct{}
	Close(context interface{}) error
}

// Subscriber defines an interface for routes to be fired upon when matched.
type Subscriber interface {
	Fire(context interface{}, params map[string]string, payload interface{}) error
}

// Router defines a interface for a route provider which registers subscriptions
// for specific paths.
type Router interface {
	Routes() [][]byte
	RoutesFor(sub Subscriber) ([][]byte, error)
	Register(path []byte, sub Subscriber) error
	Unregister(path []byte, sub Subscriber) error
	Handle(context interface{}, path []byte, payload interface{})
}

// Connections defines an interface for sending messages to two classes of
// listeners, which are clients and clusters. This allows a flexible system for
// expanding more details from a central controller or within a decentral
// controller.
type Connections interface {
	Clients(context interface{}) SearchableInfo
	Clusters(context interface{}) SearchableInfo
	SendToClients(context interface{}, msg []byte, flush bool) error
	SendToClusters(context interface{}, msg []byte, flush bool) error
}

// ConnectionEvents defines a interface which defines a connection event
// propagator.
type ConnectionEvents interface {
	OnConnect(fn func(Provider))
	OnDisconnect(fn func(Provider))
	FireConnect(Provider)
	FireDisconnect(Provider)
}

// NewBaseEvent returns a new instance of a base event.
func NewBaseEvent() *BaseEvents {
	var be BaseEvents
	return &be
}

// BaseEvents defines a struct which implements the  ConnectionEvents interface.
type BaseEvents struct {
	mc            sync.RWMutex
	onDisconnects []func(Provider)
	onConnects    []func(Provider)
}

// OnDisonnect adds a function to be called on a client connection disconnect.
func (c *BaseEvents) OnDisconnect(fn func(Provider)) {
	c.mc.Lock()
	c.onDisconnects = append(c.onDisconnects, fn)
	c.mc.Unlock()
}

// OnConnect adds a function to be called on a new client connection.
func (c *BaseEvents) OnConnect(fn func(Provider)) {
	c.mc.Lock()
	c.onConnects = append(c.onConnects, fn)
	c.mc.Unlock()
}

// FireConnect passes the provider to all disconnect handlers.
func (c *BaseEvents) FireDisconnect(p Provider) {
	c.mc.RLock()
	for _, cnFN := range c.onDisconnects {
		cnFN(p)
	}
	c.mc.RUnlock()
}

// FireConnect passes the provider to all connect handlers.
func (c *BaseEvents) FireConnect(p Provider) {
	c.mc.RLock()
	for _, cnFN := range c.onConnects {
		cnFN(p)
	}
	c.mc.RUnlock()
}

// SearchableInfo defines a BaseInfo slice which allows querying specific data
// from giving info.
type SearchableInfo []BaseInfo

// GetInfosByIP searches if the giving address and port exists within the info list
// returning the info that matches it.
func (s SearchableInfo) GetInfosByIP(ip string) ([]BaseInfo, error) {
	var infos []BaseInfo

	for _, info := range s {
		if info.IP != ip {
			continue
		}

		infos = append(infos, info)
	}

	return infos, nil
}

// GetAddr searches if the giving address and port exists within the info list
// returning the info that matches it.
func (s SearchableInfo) HasAddr(addr string, port int) (BaseInfo, error) {
	var info BaseInfo

	for _, info = range s {
		if info.Addr == addr || info.Port == port {
			break
		}
	}

	return info, nil
}

// HasInfo returns true if the info exists within the lists.
func (s SearchableInfo) HasInfo(target BaseInfo) bool {
	for _, info := range s {
		if info.Addr == target.Addr && info.Port == target.Port {
			return true
		}
	}

	return false
}
