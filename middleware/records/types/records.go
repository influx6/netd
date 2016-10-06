package types

// Versions provides an interface for a sytem to generate the version provider
// which generates versions and matches versions for operations.
type Versions interface {
	New()
	Validate(string) error
	Test(target string, newVersion string) error
}

// Cache defines the interface for all cache items which will be used in the caching record values.
type Cache interface {
	Put(Record) error
	Replace(Record) error
	Exists(recordName string, recordID string) bool
	Delete(recordName string, recordID string) error
	Get(recordName string, recordId string) (Record, error)
	Patch(DeltaRequest) (Record, error)
	GetPaths(delta DeltaRequest) (Record, error)
}

// Backend defines an interface which allows exposing a front through which records
// can be retrieved for caching and responses for operations.
type Backend interface {
	Put(Record) error
	Update(Record) (Record, error)
	All(page int, size int) ([]Record, error)
	Exists(recordName string, recordID string) bool
	Get(recordName string, id string) (Record, error)
	Delete(recordName string, recordID string) (Record, error)
}

// Delta defines a base level data store which contains a replace/add instruction
// for a Record.
type Delta struct {
	Path     string `json:"path"`
	NewValue string `json:"new_value,omitempty"`
}

// Record defines a generic interface which defines the underline record type received.
type Record struct {
	Version string                 `json:"version"`
	ID      string                 `json:"record_id"`
	Name    string                 `json:"record_name"`
	Deleted bool                   `json:"deleted"`
	Data    map[string]interface{} `json:"record_data"`
}

// DeltaRequest defines the request received from clients to perform a delta read/update operation.
type DeltaRequest struct {
	Version string  `json:"version"`
	ID      string  `json:"record_id"`
	Name    string  `json:"record_name"`
	Deleted bool    `json:"deleted"`
	Deltas  []Delta `json:"deltas"`
}

// BaseRequest defines the response/request recieved for a specific operation.
type BaseRequest struct {
	ServerID string `json:"server_id"`
	ClientID string `json:"client_id"`
	Record   Record `json:"records"`
}

// DeltaReadRequest defines the response/request recieved for a read operation.
type DeltaReadRequest struct {
	Name      string   `json:"record_name"`
	Records   []string `json:"records"`
	Deltas    []Delta  `json:"deltas"`
	Status    bool     `json:"status"`
	Processed bool     `json:"processed"`
	ServerID  string   `json:"server_id"`
	ClientID  string   `json:"client_id"`
}

// ReadRequest defines the response/request recieved for a read operation.
type ReadRequest struct {
	Records   []string `json:"records"`
	Name      string   `json:"record_name"`
	Status    bool     `json:"status"`
	Processed bool     `json:"processed"`
	ServerID  string   `json:"server_id"`
	ClientID  string   `json:"client_id"`
}

// DeleteRequest defines the request recieved for a delete operation.
type DeleteRequest struct {
	Name      string `json:"record_name"`
	Version   string `json:"version"`
	DeleteID  string `json:"delete_id"`
	Status    bool   `json:"status"`
	Processed bool   `json:"processed"`
	ServerID  string `json:"server_id"`
	ClientID  string `json:"client_id"`
}

// BaseResponse defines the response/request recieved for a specific operation.
type BaseResponse struct {
	ServerID  string `json:"server_id"`
	ClientID  string `json:"client_id"`
	Status    bool   `json:"status,omitempty"`
	Processed bool   `json:"processed,omitempty"`
	Record    Record `json:"records"`
	Error     string `json:"error,omitempty"`
}

// ReplaceResponse defines the response/request recieved for a replace operation.
type ReplaceResponse struct {
	New       Record `json:"new"`
	Old       Record `json:"old"`
	Status    bool   `json:"status"`
	Processed bool   `json:"processed"`
	ServerID  string `json:"server_id"`
	ClientID  string `json:"client_id"`
}

// DeltaResponse defines the response/request data recieved for a patch operation.
type DeltaResponse struct {
	Status    bool    `json:"status"`
	Processed bool    `json:"processed"`
	ServerID  string  `json:"server_id"`
	ClientID  string  `json:"client_id"`
	Error     string  `json:"error,omitempty"`
	Updated   Record  `json:"updated"`
	Deltas    []Delta `json:"deltas"`
}
