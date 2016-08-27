package relay

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func makeErr(message string, data ...interface{}) []byte {
	msg := []byte(fmt.Sprintf(message, data...))
	return bytes.Join([][]byte{errMessage, spaceString, msg, ctrlLine}, emptyString)
}

func toJSON(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func stripCTRL(msg []byte) []byte {
	return bytes.TrimSuffix(msg, ctrlLine)
}
