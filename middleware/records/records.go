package records

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/influx6/netd"
	"github.com/influx6/netd/middleware/records/types"
)

var (
	// ErrRecordExists is returned when a operation requires a non existing record
	// before running but finds the record id already exists.
	ErrRecordExists = errors.New("Record with given ID already exists")

	// ErrNoRecordFound is returned when an operation expects a record existing
	// with the provided id but was not found in cache or backend.
	ErrNoRecordFound = errors.New("Record with given ID already exists")

	// ErrInvalidPayloadState is returned when a invalid end state is received when no
	// begin phase is found.
	ErrInvalidPayloadState = errors.New("Invalid end message received")

	// ErrCanNotProcess is returned when an operation is not processable.
	ErrCanNotProcess = errors.New("Incapable to process operation")

	// ErrVersionConflict is returned when the record version does not match, as the
	// records are required to have a always incrementing version to reduce and
	// ensure consistency in transformations and operations on records.
	ErrVersionConflict = errors.New("Record versions are in conflict unable to perform operation")

	// ErrInvalidDeltaRecord is returned when the giving record provided ends up
	// being not a delta record type, indicated by the IsDelta flag/field.
	ErrInvalidDeltaRecord = errors.New("Record has no deltas")

	// RecordResponseMessage is used to send a reply back to the requestee after
	// processing the needed requests.
	RecordResponseMessage = []byte("RECORDRES")

	// CreateMessage defines the header name for create requests.
	CreateMessage = []byte("CREATE")

	// PatchMessage defines the header name for patch requests.
	PatchMessage = []byte("PATCH")

	// ReplaceMessage defines the header name for replace requests.
	ReplaceMessage = []byte("REPLACE")

	// ReadMessage defines the header name for read requests.
	ReadMessage = []byte("READ")

	// DeleteMessage defines the header name for delete requests.
	DeleteMessage = []byte("DELETE")
)

// bufferRecord defines a base level record buffering collector useful for operations
// that requires collection of streaming data.
type bufferRecord struct {
	bu bytes.Buffer
	on bool
}

// RecordMW returns a delegation which implements the netd.Middleware interface.
// It provides a new message processor for a RECORD message format that allows
// clients to send efficient record/model transactions over the wire.
func RecordMW(tracer netd.Trace, logger netd.Logger, versions types.Versions, cache types.Cache, backend types.Backend, ev ...netd.EventHandler) netd.Delegation {

	var createBuffer bufferRecord
	var replaceBuffer bufferRecord
	var deleteBuffer bufferRecord
	var readBuffer bufferRecord
	var patchBuffer bufferRecord

	du := netd.NewDelegation()

	// CREATE handles all create requests from the backend to create a record.
	du.Action("CREATE", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.CREATE", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.CREATE", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			createBuffer.on = true
			for _, mdata := range m.Data[1:] {
				createBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.CREATE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				createBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.CREATE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !createBuffer.on {
				logger.Error(context, "RecordMW.CREATE", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			createBuffer.on = false
		default:
			for _, mdata := range m.Data {
				createBuffer.bu.Write(mdata)
			}
		}

		data := createBuffer.bu.Bytes()
		createBuffer.bu.Reset()

		var rec types.BaseRequest
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		if err := versions.Validate(rec.Record.Version); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		// Check if record already exists in cache, if so then these means its violated
		// new record policy for create.
		if cache.Exists(rec.Record.Name, rec.Record.ID) {
			logger.Error(context, "RecordMW.CREATE", ErrRecordExists, "Completed")
			return nil, true, ErrRecordExists
		}

		if backend.Exists(rec.Record.Name, rec.Record.ID) {

			rec, err := backend.Get(rec.Record.Name, rec.Record.ID)
			if err != nil {
				logger.Error(context, "RecordMW.CREATE", err, "Completed")
				return nil, true, err
			}

			if err := cache.Put(rec); err != nil {
				logger.Error(context, "RecordMW.CREATE", err, "Completed")
				return nil, true, err
			}

			return nil, true, ErrRecordExists
		}

		// Store record in backend and if error'd out then return err and close
		// connection.
		if err := backend.Put(rec.Record); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		// Store record in cache for quick access.
		if err := cache.Put(rec.Record); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		responseJSON, err := json.Marshal(&types.BaseResponse{
			Status:    true,
			Processed: true,
			Record:    rec.Record,
			ClientID:  cx.Base.ClientID,
			ServerID:  cx.Base.ServerID,
		})

		if err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		topic := bytes.Join([][]byte{
			[]byte("records"),
			[]byte(rec.Record.Name),
			bytes.ToLower(CreateMessage),
		}, []byte("."))
		cx.Router.Handle(context, topic, responseJSON, *cx.Base)

		res := netd.WrapResponseBlock(RecordResponseMessage, CreateMessage, responseJSON)
		if err := cx.SendToClusters(context, cx.Base.ClientID, res, true); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Failed to send to clusters")
		}

		logger.Log(context, "RecordMW.CREATE", "Completed")
		return res, false, nil
	})

	// REPLACE handles all replace requests from the backend to create a record.
	du.Action("REPLACE", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.REPLACE", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.REPLACE", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			replaceBuffer.on = true
			for _, mdata := range m.Data[1:] {
				replaceBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.REPLACE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				replaceBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.REPLACE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !replaceBuffer.on {
				logger.Error(context, "RecordMW.REPLACE", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			replaceBuffer.on = false
		default:
			for _, mdata := range m.Data {
				replaceBuffer.bu.Write(mdata)
			}
		}

		data := replaceBuffer.bu.Bytes()
		replaceBuffer.bu.Reset()

		var rec types.BaseRequest
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		// Validate the version of the record request if it matches the standard for our
		// versioner.
		if err := versions.Validate(rec.Record.Version); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		var err error
		var oldRecord types.Record

		if cache.Exists(rec.Record.Name, rec.Record.ID) {

			oldRecord, err = cache.Get(rec.Record.Name, rec.Record.ID)
			if err != nil {
				logger.Error(context, "RecordMW.REPLACE", err, "Completed")
				return nil, true, err
			}

		} else {

			if !backend.Exists(rec.Record.Name, rec.Record.ID) {
				return nil, true, ErrNoRecordFound
			}

			oldRecord, err = backend.Get(rec.Record.Name, rec.Record.ID)
			if err != nil {
				logger.Error(context, "RecordMW.REPLACE", err, "Completed")
				return nil, true, err
			}

		}

		// Request the version of the OldRecord be tested against the new record request
		// if the versions are equal or the new request is older than the current version then
		// can not allow this operation to continue.
		if err := versions.Test(oldRecord.Version, rec.Record.Version); err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		// Store record in backend and if error'd out then return err and close
		// connection.
		newRecord, err := backend.Update(rec.Record)
		if err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		// Store record in cache for quick access.
		if err := cache.Replace(newRecord); err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		responseJSON, err := json.Marshal(&types.ReplaceResponse{
			Status:    true,
			Processed: true,
			Old:       oldRecord,
			New:       newRecord,
			ClientID:  cx.Base.ClientID,
			ServerID:  cx.Base.ServerID,
		})

		if err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		topic := bytes.Join([][]byte{
			[]byte("records"),
			[]byte(rec.Record.Name),
			bytes.ToLower(ReplaceMessage),
		}, []byte("."))
		cx.Router.Handle(context, topic, responseJSON, *cx.Base)

		res := netd.WrapResponseBlock(RecordResponseMessage, ReplaceMessage, responseJSON)
		if err := cx.SendToClusters(context, cx.Base.ClientID, res, true); err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Failed to send to clusters")
		}

		logger.Log(context, "RecordMW.REPLACE", "Completed")
		return res, false, nil
	})

	// DELETE handles all replace requests from the backend to create a record.
	du.Action("DELETE", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.DELETE", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.DELETE", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			deleteBuffer.on = true
			for _, mdata := range m.Data[1:] {
				deleteBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.DELETE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				deleteBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.DELETE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !deleteBuffer.on {
				logger.Error(context, "RecordMW.DELETE", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			deleteBuffer.on = false
		default:
			for _, mdata := range m.Data {
				deleteBuffer.bu.Write(mdata)
			}
		}

		data := deleteBuffer.bu.Bytes()
		deleteBuffer.bu.Reset()

		var rec types.DeleteRequest
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.DELETE", err, "Completed")
			return nil, true, err
		}

		// Validate the version of the record request if it matches the standard for our
		// versioner.
		if err := versions.Validate(rec.Version); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		var foundInCache bool
		if cache.Exists(rec.Name, rec.DeleteID) {

			cacheRec, err := cache.Get(rec.Name, rec.DeleteID)
			if err != nil {
				logger.Error(context, "RecordMW.DELETE", err, "Completed")
				return nil, true, err
			}

			// Request the version of the OldRecord be tested against the new record request
			// if the versions are equal or the new request is older than the current version then
			// can not allow this operation to continue.
			if err := versions.Test(cacheRec.Version, rec.Version); err != nil {
				logger.Error(context, "RecordMW.Delete", err, "Completed")
				return nil, true, err
			}

			foundInCache = true

		} else {

			cacheRec, err := backend.Get(rec.Name, rec.DeleteID)
			if err != nil {
				logger.Error(context, "RecordMW.DELETE", err, "Completed")
				return nil, true, err
			}

			// Request the version of the OldRecord be tested against the new record request
			// if the versions are equal or the new request is older than the current version then
			// can not allow this operation to continue.
			if err := versions.Test(cacheRec.Version, rec.Version); err != nil {
				logger.Error(context, "RecordMW.Delete", err, "Completed")
				return nil, true, err
			}

		}

		backendRec, err := backend.Delete(rec.Name, rec.DeleteID)
		if err != nil {
			logger.Error(context, "RecordMW.DELETE", err, "Info : Delete Failed for ID[%s]", rec.DeleteID)
			return nil, true, err
		}

		if foundInCache {
			if err := cache.Delete(rec.Name, rec.DeleteID); err != nil {
				logger.Error(context, "RecordMW.DELETE", err, "Completed")
				return nil, true, err
			}
		}

		responseJSON, err := json.Marshal(&types.BaseResponse{
			Status:    true,
			Processed: true,
			Record:    backendRec,
			ClientID:  cx.Base.ClientID,
			ServerID:  cx.Base.ServerID,
		})

		if err != nil {
			logger.Error(context, "RecordMW.DELETE", err, "Completed")
			return nil, true, err
		}

		topic := bytes.Join([][]byte{
			[]byte("records"),
			[]byte(rec.Name),
			bytes.ToLower(DeleteMessage),
		}, []byte("."))
		cx.Router.Handle(context, topic, responseJSON, *cx.Base)

		res := netd.WrapResponseBlock(RecordResponseMessage, DeleteMessage, responseJSON)
		if err := cx.SendToClusters(context, cx.Base.ClientID, res, true); err != nil {
			logger.Error(context, "RecordMW.DELETE", err, "Failed to send to clusters")
		}

		logger.Log(context, "RecordMW.DELETE", "Completed")
		return res, false, nil
	})

	// GET/READ handles all read requests from the backend to create a record.
	du.Action("READ", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.READ", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.READ", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			readBuffer.on = true
			for _, mdata := range m.Data[1:] {
				readBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.READ", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				readBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.READ", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !readBuffer.on {
				logger.Error(context, "RecordMW.READ", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			readBuffer.on = false
		default:
			for _, mdata := range m.Data {
				readBuffer.bu.Write(mdata)
			}
		}

		data := readBuffer.bu.Bytes()
		readBuffer.bu.Reset()

		var rec types.ReadRequest
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.READ", err, "Completed")
			return nil, true, err
		}

		var records []types.BaseResponse

		for _, id := range rec.Records {
			var status bool

			if cache.Exists(rec.Name, id) {

				record, err := cache.Get(rec.Name, id)
				if err == nil {
					status = true
				}

				records = append(records, types.BaseResponse{
					Status:    status,
					Record:    record,
					Processed: true,
					Error:     err.Error(),
					ServerID:  cx.Base.ServerID,
					ClientID:  cx.Base.ClientID,
				})

				continue
			}

			record, err := backend.Get(rec.Name, id)
			if err == nil {
				status = true
			}

			records = append(records, types.BaseResponse{
				Status:    status,
				Record:    record,
				Processed: true,
				Error:     err.Error(),
				ServerID:  cx.Base.ServerID,
				ClientID:  cx.Base.ClientID,
			})
		}

		responseJSON, err := json.Marshal(&records)
		if err != nil {
			logger.Error(context, "RecordMW.READ", err, "Completed")
			return nil, true, err
		}

		topic := bytes.Join([][]byte{
			[]byte("records"),
			[]byte(rec.Name),
			bytes.ToLower(DeleteMessage),
		}, []byte("."))
		cx.Router.Handle(context, topic, responseJSON, *cx.Base)

		res := netd.WrapResponseBlock(RecordResponseMessage, ReadMessage, responseJSON)
		if err := cx.SendToClusters(context, cx.Base.ClientID, res, true); err != nil {
			logger.Error(context, "RecordMW.READ", err, "Failed to send to clusters")
		}

		logger.Log(context, "RecordMW.READ", "Completed")
		return res, false, nil
	})

	// PATCH handles all patch requests from the backend to patch/update a record.
	du.Action("PATCH", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.PATCH", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.PATCH", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			patchBuffer.on = true
			for _, mdata := range m.Data[1:] {
				patchBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.PATCH", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				patchBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.PATCH", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !patchBuffer.on {
				logger.Error(context, "RecordMW.PATCH", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			patchBuffer.on = false
		default:
			for _, mdata := range m.Data {
				patchBuffer.bu.Write(mdata)
			}
		}

		data := patchBuffer.bu.Bytes()
		patchBuffer.bu.Reset()

		var rec types.DeltaRequest
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Completed")
			return nil, true, err
		}

		// If we are not dealing with Delta Records then we must not service the
		// request.
		if len(rec.Deltas) == 0 {
			logger.Error(context, "RecordMW.PATCH", ErrInvalidDeltaRecord, "Completed")
			return nil, true, ErrInvalidDeltaRecord
		}

		// Validate the version of the record request if it matches the standard for our
		// versioner.
		if err := versions.Validate(rec.Version); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		var err error
		var cacheRecord types.Record

		if cache.Exists(rec.Name, rec.ID) {

			cacheRecord, err = cache.Get(rec.Name, rec.ID)
			if err != nil {

				// If we are not able to get the details out of the cache, then
				// get it from the backend then replace in cache and retry.
				cacheRecord, err = backend.Get(rec.Name, rec.ID)
				if err != nil {
					logger.Error(context, "RecordMW.PATCH", err, "Completed")
					return nil, true, err
				}

				if err := cache.Replace(cacheRecord); err != nil {
					logger.Error(context, "RecordMW.PATCH", err, "Completed")
					return nil, true, err
				}
			}

		} else {

			// If we do not have record in cache, then retrieve and add into cache.
			// Then call cache.Patch and store the new record if no error into
			// backend.
			cacheRecord, err = backend.Get(rec.Name, rec.ID)
			if err != nil {
				logger.Error(context, "RecordMW.PATCH", err, "Completed")
				return nil, true, err
			}

			if err := cache.Put(cacheRecord); err != nil {
				logger.Error(context, "RecordMW.PATCH", err, "Completed")
				return nil, true, err
			}

		}

		if err := versions.Test(cacheRecord.Version, rec.Version); err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Completed")
			return nil, true, err
		}

		patchedRecord, err := cache.Patch(rec)
		if err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Completed")
			return nil, true, err
		}

		if _, err := backend.Update(patchedRecord); err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Completed")
			return nil, true, err
		}

		if err := cache.Replace(patchedRecord); err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Completed")
			return nil, true, err
		}

		responseJSON, err := json.Marshal(&types.BaseResponse{
			Status:    true,
			Record:    patchedRecord,
			Processed: true,
			ServerID:  cx.Base.ServerID,
			ClientID:  cx.Base.ClientID,
		})

		if err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Completed")
			return nil, true, err
		}

		topic := bytes.Join([][]byte{
			[]byte("records"),
			[]byte(rec.Name),
			bytes.ToLower(PatchMessage),
		}, []byte("."))
		cx.Router.Handle(context, topic, responseJSON, *cx.Base)

		res := netd.WrapResponseBlock(RecordResponseMessage, ReplaceMessage, responseJSON)
		if err := cx.SendToClusters(context, cx.Base.ClientID, res, true); err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Failed to send to clusters")
		}

		logger.Log(context, "RecordMW.PATCH", "Completed")
		return res, false, nil
	})

	des := netd.NewDelegation()
	des.Action("CREATE", func(context interface{}, m netd.Message, cx *netd.Connection)([]byte, bool, error){})
	des.Action("UPDATE", func(context interface{}, m netd.Message, cx *netd.Connection)([]byte, bool, error){})
	des.Action("CREATE", func(context interface{}, m netd.Message, cx *netd.Connection)([]byte, bool, error){})
	des.Action("CREATE", func(context interface{}, m netd.Message, cx *netd.Connection)([]byte, bool, error){})

	return netd.NewDelegation(ev...).
		Next("RECORDS", du).
		Next("RECORDRES", dres).
}

//==============================================================================
