package log

import (
	"bytes"
	"fmt"
	"io"
)

// Logger defines a base logger which logs all into a provided writer.
type Logger struct {
	in io.Writer
}

// NewLogger returns a new Logger instance.
func NewLogger(in io.Writer) *Logger {
	return &Logger{in: in}
}

// Log provides a normal-level log which allows different levels to be log with.
func (l *Logger) Log(context interface{}, fn string, message string, data ...interface{}) {
	fmt.Fprintf(l.in, "Context[%s] :  Function[%s] :  %s\n", context, fn, fmt.Sprintf(message, data...))
}

// Error provides a error-level log function.
func (l *Logger) Error(context interface{}, fn string, err error, message string, data ...interface{}) {
	fmt.Fprintf(l.in, "Context[%s] :  Function[%s] :  %s : Error[%s]\n", context, fn, fmt.Sprintf(message, data...), err)
}

// Tracer defines a data trace provider which uses a internal buffer to collect all trace then
// clears when ended.
type Tracer struct {
	b  bytes.Buffer
	in io.Writer
}

// NewTracer returns a new instance of a tracer.
func NewTracer(in io.Writer) *Tracer {
	var t Tracer
	t.in = in
	return &t
}

// Begin begins the trace call for the giving message.
func (t *Tracer) Begin(context interface{}, msg []byte) {
	t.b.Write([]byte(fmt.Sprintf("--TRACE [%+q] Begin------------------------------", msg)))
}

// Trace logs the trace call for the giving message.
func (t *Tracer) Trace(context interface{}, msg []byte) {
	t.b.Write(msg)
}

// End ends the trace call for the giving message and clears the internal buffer, writing the trace
// into the internal writer.
func (t *Tracer) End(context interface{}, msg []byte) {
	t.b.Write([]byte(fmt.Sprintf("--TRACE [%+q] Ended------------------------------", msg)))
	t.in.Write(t.b.Bytes())
	t.b.Reset()
}
