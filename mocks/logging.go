package mocks

import (
	"bytes"
	"fmt"
	"io"
)

// MockLogger defines a base logger which logs all into a provided writer.
type MockLogger struct {
	in io.Writer
}

// NewMockLogger returns a new Logger instance.
func NewMockLogger(in io.Writer) *MockLogger {
	return &MockLogger{in: in}
}

// Log provides a normal-level log which allows different levels to be log with.
func (l *MockLogger) Log(context interface{}, fn string, message string, data ...interface{}) {
	fmt.Fprintf(l.in, "Context[%s] :  Function[%s] :  %s\n", context, fn, fmt.Sprintf(message, data...))
}

// Error provides a error-level log function.
func (l *MockLogger) Error(context interface{}, fn string, err error, message string, data ...interface{}) {
	fmt.Fprintf(l.in, "Context[%s] :  Function[%s] :  %s : Error[%s]\n", context, fn, fmt.Sprintf(message, data...), err)
}

// MockTracer defines a data trace provider which uses a internal buffer to collect all trace then
// clears when ended.
type MockTracer struct {
	b  bytes.Buffer
	in io.Writer
}

// NewTracerMock returns a new instance of a tracer.
func NewMockTracer(in io.Writer) *MockTracer {
	var t MockTracer
	t.in = in
	return &t
}

// Begin begins the trace call for the giving message.
func (t *MockTracer) Begin(context interface{}, msg []byte) {
	t.b.Write([]byte(fmt.Sprintf("--TRACE [%+q] Begin------------------------------\n", msg)))
}

// Trace logs the trace call for the giving message.
func (t *MockTracer) Trace(context interface{}, msg []byte) {
	t.b.Write(msg)
	t.b.Write([]byte("\n"))
}

// End ends the trace call for the giving message and clears the internal buffer, writing the trace
// into the internal writer.
func (t *MockTracer) End(context interface{}, msg []byte) {
	t.b.Write([]byte(fmt.Sprintf("--TRACE [%+q] Ended------------------------------\n", msg)))
	t.in.Write(t.b.Bytes())
	t.b.Reset()
}
