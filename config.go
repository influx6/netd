package netd

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"
)

//==============================================================================

// Trace defines an interface which receives data trace data logs.
type Trace interface {
	Begin(context interface{}, msg []byte)
	Trace(context interface{}, msg []byte)
	End(context interface{}, msg []byte)
}

// Tracer defines a empty tracer struct which allows a inplace tracer for when
// tracing is disabled but still called in code.
var Tracer tracer

type tracer struct{}

func (tracer) Begin(context interface{}, msg []byte) {}
func (tracer) Trace(context interface{}, msg []byte) {}
func (tracer) End(context interface{}, msg []byte)   {}

// Logger defines an interface which receives logs events/messages.
type Logger interface {
	Log(context interface{}, targetFunc string, message string, data ...interface{})
	Error(context interface{}, targetFunc string, err error, message string, data ...interface{})
}

// Logger defines an empty logger which can be used in place for when logging is
// is not set.
var Log logger

type logger struct{}

func (logger) Log(context interface{}, target string, message string, data ...interface{}) {}
func (logger) Error(context interface{}, target string, err error, message string, data ...interface{}) {
}

//==============================================================================

// Crendential defines a struct for storing user authentication crendentials.
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Config provides a configuration struct which defines specific settings for
// the connection handler.
type Config struct {
	Trace
	Logger

	ClientCrendentails []Credential `json:"-"`

	Port int    `json:"port"`
	Addr string `json:"addr"`

	HTTPPort  int    `json:"http_port"`
	HTTPAddr  string `json:"http_addr"`
	HTTPSPort int    `json:"https_port"`
	HTTPSAddr string `json:"https_addr"`

	ClusterCredentials    []Credential `json:"-"`
	ClustersPort          int          `json:"clusters_port"`
	ClustersAddr          string       `json:"clusters_addr"`
	MaxClusterAuthTimeout float64      `json:"max_cluster_auth_timeout"`

	MaxPayload      int64         `json:"max_payload_size"`
	MaxPending      int64         `json:"max_pending_size"`
	MaxConnections  int           `json:"max_connections"`
	MaxPingInterval time.Duration `json:"max_ping_timeout"`
	MaxPingTimeout  float64       `json:"max_ping_timeout"`

	Authenticate     bool `json:"authenticate"`
	MustAuthenticate bool `json:"must_authenticate"`

	ClientAuth  Auth `json:"-"`
	ClusterAuth Auth `json:"-"`

	UseTLS        bool        `json:"use_tls"`
	MaxTLSTimeout float64     `json:"max_tls_timeout"`
	TLSKeyFile    string      `json:"-"`
	TLSCertFile   string      `json:"-"`
	TLSCaCertFile string      `json:"-"`
	TLSVerify     bool        `json:"TLSVerify"`
	TLSConfig     *tls.Config `json:"-"`
}

// InitLogAndTrace checks and assigns dummy log and trace callers to the config
// if that was not set to ensure calls get passed through without panics.
func (c *Config) InitLogAndTrace() {
	if c.Logger == nil {
		c.Logger = Log
	}
	if c.Trace == nil {
		c.Trace = Tracer
	}
}

// MatchClientCredentials matches the provided crendential against the
// provided static users crendential, this is useful for testing as it
// allows a predefined set of crendentails to allow.
func (c *Config) MatchClientCredentials(cd Credential) bool {
	for _, user := range c.ClientCrendentails {
		if cd.Username == user.Username && cd.Password == user.Password {
			return true
		}
	}

	return false
}

// MatchClusterCredentials matches the provided crendential against the
// provided static cluster users crendential, this is useful for testing as it
// allows a predefined set of crendentails to allow.
func (c *Config) MatchClusterCredentials(cd Credential) bool {
	for _, user := range c.ClusterCredentials {
		if cd.Username == user.Username && cd.Password == user.Password {
			return true
		}
	}

	return false
}

// ParseTLS parses the tls configuration variables assigning the value to the
// TLSConfig if not already assigned to.
func (c *Config) ParseTLS() error {
	if c.TLSConfig != nil || !c.UseTLS {
		return nil
	}

	var err error
	c.TLSConfig, err = LoadTLS(c.TLSCertFile, c.TLSKeyFile, c.TLSCaCertFile)
	if err != nil {
		return err
	}

	return nil
}

//==============================================================================

// BaseInfo provides a struct which contains important data about the server
// which is providing the connection handling.
type BaseInfo struct {
	Addr             string `json:"addr"`
	Port             int    `json:"port"`
	LocalAddr        string `json:"local_addr"`
	LocalPort        int    `json:"local_port"`
	RealAddr         string `json:"real_addr"`
	RealPort         int    `json:"real_port"`
	ServerID         string `json:"server_id"`
	ClientID         string `json:"client_id"`
	Version          string `json:"version"`
	GoVersion        string `json:"go_version"`
	IP               string `json:"ip,emitempty"`
	MaxPayload       int    `json:"max_payload"`
	ClusterNode      bool   `json:"cluster_node"`
	ConnectInitiator bool   `json:"-"`
	HandleReconnect  bool   `json:"-"`
}

// FromMap collects the  needed information for the baseInfo from the provided map.
func (b *BaseInfo) FromMap(info map[string]interface{}) {
	if port, ok := info["port"].(float64); ok {
		b.Port = int(port)
	}

	if port, ok := info["local_port"].(float64); ok {
		b.LocalPort = int(port)
	}

	if port, ok := info["real_port"].(float64); ok {
		b.RealPort = int(port)
	}

	if mx, ok := info["max_payload"].(float64); ok {
		b.MaxPayload = int(mx)
	}

	if p, ok := info["addr"].(string); ok {
		b.Addr = p
	}

	if p, ok := info["real_addr"].(string); ok {
		b.RealAddr = p
	}

	if p, ok := info["local_addr"].(string); ok {
		b.LocalAddr = p
	}

	if p, ok := info["ip"].(string); ok {
		b.IP = p
	}

	if p, ok := info["version"].(string); ok {
		b.Version = p
	}

	if p, ok := info["go_version"].(string); ok {
		b.GoVersion = p
	}

	if p, ok := info["server_id"].(string); ok {
		b.ServerID = p
	}

	if p, ok := info["client_id"].(string); ok {
		b.ClientID = p
	}

	if cn, ok := info["cluster_node"].(bool); ok {
		b.ClusterNode = cn
	}
}

// MatchLocalAddr the provided info with the base info.
func (b BaseInfo) MatchLocalAddr(info BaseInfo) bool {
	if (b.Addr == "" || b.Addr == "0.0.0.0") && b.LocalPort == info.Port {
		return true
	}

	if b.Addr == info.LocalAddr && b.Port == info.LocalPort {
		return true
	}

	if b.LocalAddr != "" && b.LocalPort != 0 {
		if b.LocalAddr == info.LocalAddr && b.LocalPort == info.LocalPort {
			return true
		}
	}

	return false
}

// MatchSignature returns true/false if the info signatures match each other.
func (b BaseInfo) MatchSignature(info BaseInfo) bool {
	return (b.ServerID == info.ServerID && b.ClientID == info.ClientID)
}

// MatchAddr the provided info with the base info.
func (b BaseInfo) MatchAddr(info BaseInfo) bool {
	if b.RealAddr == info.Addr && b.RealPort == info.Port {
		return true
	}

	if b.RealAddr == info.RealAddr && b.RealPort == info.RealPort {
		return true
	}

	if b.Addr == info.Addr && b.Port == info.Port {
		return true
	}

	return false
}

// Match the provided info with the base info.
func (b BaseInfo) Match(info BaseInfo) bool {
	return b.MatchAddr(info) || b.MatchSignature(info)
}

// ID returns a more terse string relating to the given baseinfo.
func (b BaseInfo) ID() string {
	return fmt.Sprintf(`
  ClientID: %s
  ServerID: %s
  Addr: "%s:%d"
  LocalAddr: "%s:%d"
`, b.ClientID, b.ServerID, b.Addr, b.Port, b.LocalAddr, b.LocalPort)
}

// String returns a json parsed version of the BaseInfo.
func (b BaseInfo) String() string {
	jsn, err := json.Marshal(b)
	if err != nil {
		return ""
	}

	return string(jsn)
}
