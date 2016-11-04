package router

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/influx6/netd"
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

func (r *redbell) Fire(context interface{}, msg *netd.SubMessage) error {
	fmt.Printf("Context[%#v] : Payload[%+q] : Red bell just rang\n", context, msg)
	r.c.Done()
	return nil
}

type redblackbell struct{ c *counter }

func (r *redblackbell) Fire(context interface{}, msg *netd.SubMessage) error {
	fmt.Printf("Context[%#v] : Payload[%+q] : RedBlack bell just rang\n", context, msg)
	r.c.Done()
	return nil
}

type blackbell struct{ c *counter }

func (b *blackbell) Fire(context interface{}, msg *netd.SubMessage) error {
	fmt.Printf("Context[%#v] : Payload[%+q] : Black bell just rang\n", context, msg)
	b.c.Done()
	return nil
}

type rootbell struct{ c *counter }

func (r *rootbell) Fire(context interface{}, msg *netd.SubMessage) error {
	fmt.Printf("Context[%#v] : Payload[%+q] : Root bell just rang\n", context, msg)
	r.c.Done()
	return nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func TestStrictRoutes(t *testing.T) {
	alarm := routes.New()

	c := &counter{}
	c.Add()

	if !c.Match(1) {
		fatalFailed(t, "Should have successfully increased counter to 1: %s", c)
	}
	logPassed(t, "Should have successfully increased counter to 1: %s", c)

	rootSub := &rootbell{c}
	must(alarm.Register([]byte(`/`), rootSub))
	must(alarm.Register([]byte(`alarm.red`), &redbell{c}))
	must(alarm.Register([]byte(`alarm.ish^.black`), &redblackbell{c}))
	must(alarm.Register([]byte(`alarm.{color:[^black$]}`), &blackbell{c}))

	subs := alarm.Routes()
	if len(subs) != 6 {
		t.Logf("Recieved: %+s\n", subs)
		fatalFailed(t, "Should have successfully returned 4 items in the route list")
	}
	logPassed(t, "Should have successfully returned 4 items in the route list")

	path := routes.PathToByte("/")
	alarm.Handle(context, path, "RootaBalls", nil)

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %s", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %s", c)

	c.Add()

	if !c.Match(1) {
		fatalFailed(t, "Should have successfully increased counter to 1: %s", c)
	}
	logPassed(t, "Should have successfully increased counter to 1: %s", c)
	path = routes.PathToByte("/alarm/red")
	alarm.Handle(context, path, "Balls", nil)

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
	alarm.Handle(context, path, "Balls", nil)

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
	alarm.Handle(context, path, "Balls", nil)

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %d", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %d", c)

	c.Add()
	c.Add()
	c.Add()
	c.Add()
	if !c.Match(4) {
		fatalFailed(t, "Should have successfully increased counter to 3: %d", c)
	}
	logPassed(t, "Should have successfully increase counter to 3: %d", c)

	path = routes.PathToByte("*")
	alarm.Handle(context, path, "Balls", nil)

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %d", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %d", c)

	must(alarm.Unregister([]byte(`/`), rootSub))

}

func logPassed(t *testing.T, msg string, data ...interface{}) {
	t.Logf("%s %s", fmt.Sprintf(msg, data...), succeedMark)
}

func fatalFailed(t *testing.T, msg string, data ...interface{}) {
	t.Fatalf("%s %s", fmt.Sprintf(msg, data...), failedMark)
}
