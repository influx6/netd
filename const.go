package netd

import (
	"errors"
	"time"
)

const (
	// CTRL defines the ending control line which must end all messages.
	CTRL = "\r\n"

	// VERSION is the current version for the server.
	VERSION = "0.0.1"

	// DEFAULT_PORT is the deault port for client connections.
	DEFAULT_PORT = 3508

	// RANDOM_PORT is the value for port that, when supplied, will cause the
	// server to listen on a randomly-chosen available port. The resolved port
	// is available via the Addr() method.
	RANDOM_PORT = -1

	// MIN_DATA_WRITE_SIZE defines the minimum buffer writer size to be recieved by
	// the connection readers.
	MIN_DATA_WRITE_SIZE = 512

	// MAX_Data_WRITE_SIZE defines the maximum buffer writer size and data size to be
	// allowed on the connection
	MAX_DATA_WRITE_SIZE = 6048

	// DEFAULT_FLUSH_DEADLINE is the write/flush deadlines.
	DEFAULT_FLUSH_DEADLINE = 2 * time.Second

	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 10 * time.Millisecond

	// MAX_CONTROL_LINE_SIZE is the maximum allowed protocol control line size.
	// 1k should be plenty since payloads sans connect string are separate
	MAX_CONTROL_LINE_SIZE = 1024

	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 1 * time.Second

	// MAX_PAYLOAD_SIZE is the maximum allowed payload size. Should be using
	// something different if > 1MB payloads are needed.
	MAX_PAYLOAD_SIZE = (1024 * 1024)

	// MAX_PENDING_SIZE is the maximum outbound size (in bytes) per client.
	MAX_PENDING_SIZE = (10 * 1024 * 1024)

	// DEFAULT_MAX_CONNECTIONS is the default maximum connections allowed.
	DEFAULT_MAX_CONNECTIONS = (64 * 1024)

	// TLS_TIMEOUT is the TLS wait time.
	TLS_TIMEOUT = float64(500*time.Millisecond) / float64(time.Second)

	// AUTH_TIMEOUT is the authorization wait time.
	AUTH_TIMEOUT = float64(2*TLS_TIMEOUT) / float64(time.Second)

	// DEFAULT_PING_INTERVAL is how often pings are sent to clients and routes.
	DEFAULT_PING_INTERVAL = 2 * time.Minute

	// DEFAULT_DIAL_TIMEOUT is how often pings are sent to clients and routes.
	DEFAULT_DIAL_TIMEOUT = 3 * time.Second

	// DEFAULT_CLUSTER_NEGOTIATION_TIMEOUT defins the timeout for read op on cluster
	// setup.
	DEFAULT_CLUSTER_NEGOTIATION_TIMEOUT = 30 * time.Second

	// DEFAULT_PING_MAX_OUT is maximum allowed pings outstanding before disconnect.
	DEFAULT_PING_MAX_OUT = 2
)

var (
	// CTRLine defines the byte slice form of the CTRL character.
	CTRLINE = []byte(CTRL)

	// NewLine defines the byte slice for the newline character.
	NewLine = []byte("\n")

	// InfoMessage defines the info header for request connection info.
	InfoMessage = []byte("INFO")

	// OkMessage defines the header used for signifying response success.
	OkMessage = []byte("OK")

	// EndMessage defines the header send to indicate message end.
	EndMessage = []byte("+MSGED")

	// DataMessage defines the header send to indicate a published event/data.
	DataMessage = []byte("+DATA")

	// BeginMessage defines the header send to indicate message begin.
	BeginMessage = []byte("+MSGBG")

	// ClusterMessage defines the header used for signifying a new cluster.
	ClusterMessage = []byte("CLUSTER")

	// ClustersMessage defines the header used for requesting a provider cluster
	// cluster list.
	ClustersMessage = []byte("CLUSTERS")

	// ClusterRoute defines the router clusters are added to for receiving data broadcasts.
	ClusterRoute = []byte("_clusters")

	// ConnectMessage defines the header sent by a new cluster.
	ConnectMessage = []byte("CONNECT")

	// ErrMessage signifies the header used to signal error messages.
	ErrMessage = []byte("+ERR")

	// RespMessage snififies the header used to singal response messages.
	RespMessage = []byte("+RESP")

	// ErrInvalidRequest signify the error sent when an invalid request was
	// received.
	ErrInvalidRequest = errors.New("Received Invalid Request")

	// ErrInvalidResponse signify the error sent when an invalid response was
	// received.
	ErrInvalidResponse = errors.New("Received Invalid Response")

	// ErrNoResponse signify the error sent when no response was recieved within
	// defined limits.
	ErrNoResponse = errors.New("Failed to recieve response")

	// ErrInvalidInfo defines the error sent when info data was invalid json.
	ErrInvalidInfo = errors.New("Failed to unmarshal info data")

	// ErrNegotationFailed defines the error sent during a failed cluster negotiation.
	ErrNegotiationFailed = errors.New("Failed to negotiate with new cluster")

	// ErrExpectedInfo defines the error sent when info was not received during
	// a info response.
	ErrExpectedInfo = errors.New("Failed to receive info response for connect")

	// ErrInvalidClusterFormat defines the error sent when a invalid cluster request
	// is made.
	ErrInvalidClusterFormat = errors.New("Invalid Cluster Data, expected {CLUSTER|ADDR|PORT}")
)
