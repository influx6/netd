package routes_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/influx6/netd/routes"
)

const (
	// succeedMark is the Unicode codepoint for a check mark.
	succeedMark = "\u2713"

	// failedMark is the Unicode codepoint for an X mark.
	failedMark = "\u2717"
)

var context = "testing"

type counter struct {
	c int64
}

func (c *counter) Add() {
	atomic.AddInt64(&c.c, 1)
}

func (c *counter) Done() {
	atomic.AddInt64(&c.c, -1)
}

func (c *counter) String() string {
	return fmt.Sprintf("Counter(%d)", atomic.LoadInt64(&c.c))
}

func (c *counter) Match(bit int) bool {
	return int(atomic.LoadInt64(&c.c)) == bit
}

type redbell struct{ c *counter }

func (r *redbell) Fire(context interface{}, params map[string]string, payload interface{}) {
	fmt.Printf("Context[%#v] : Params[%#v] : Payload[%#v] : Red bell just rang\n", context, params, payload)
	r.c.Done()
}

type redblackbell struct{ c *counter }

func (r *redblackbell) Fire(context interface{}, params map[string]string, payload interface{}) {
	fmt.Printf("Context[%#v] : Params[%#v] : Payload[%#v] : RedBlack bell just rang\n", context, params, payload)
	r.c.Done()
}

type blackbell struct{ c *counter }

func (b *blackbell) Fire(context interface{}, params map[string]string, payload interface{}) {
	fmt.Printf("Context[%#v] : Params[%#v] : Payload[%#v] : Black bell just rang\n", context, params, payload)
	b.c.Done()
}

func TestStrictRoutes(t *testing.T) {
	alarm := routes.New()

	c := &counter{}
	c.Add()

	if !c.Match(1) {
		fatalFailed(t, "Should have successfully increased counter to 1: %s", c)
	}
	logPassed(t, "Should have successfully increaseed counter to 1: %s", c)

	alarm.MustRegister([]byte(`alarm.red`), &redbell{c})
	alarm.MustRegister([]byte(`alarm.ish^.black`), &redblackbell{c})
	alarm.MustRegister([]byte(`alarm.{color:[^black$]}`), &blackbell{c})

	path := routes.PathToByte("/alarm/red")
	alarm.Handle(context, path, "Balls")

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %s", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %s", c)

	c.Add()
	if !c.Match(1) {
		fatalFailed(t, "Should have successfully increased counter to 1: %s", c)
	}
	logPassed(t, "Should have successfully increaseed counter to 1: %s", c)

	path = routes.PathToByte("/alarm/redish/black")
	alarm.Handle(context, path, "Balls")

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %s", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %s", c)

	c.Add()
	if !c.Match(1) {
		fatalFailed(t, "Should have successfully increased counter to 1: %d", c)
	}
	logPassed(t, "Should have successfully increase counter to 1: %d", c)

	path = routes.PathToByte("/alarm/black")
	alarm.Handle(context, path, "Balls")

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %d", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %d", c)

	c.Add()
	c.Add()
	c.Add()
	if !c.Match(3) {
		fatalFailed(t, "Should have successfully increased counter to 3: %d", c)
	}
	logPassed(t, "Should have successfully increase counter to 3: %d", c)

	path = routes.PathToByte("*")
	alarm.Handle(context, path, "Balls")

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %d", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %d", c)
}

func logPassed(t *testing.T, msg string, data ...interface{}) {
	t.Logf("%s %s", fmt.Sprintf(msg, data...), succeedMark)
}

func fatalFailed(t *testing.T, msg string, data ...interface{}) {
	t.Fatalf("%s %s", fmt.Sprintf(msg, data...), failedMark)
}
