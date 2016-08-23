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

type counter int64

func (c counter) Add() {
	vc := int64(c)
	atomic.AddInt64(&vc, 1)
}

func (c counter) Done() {
	vc := int64(c)
	atomic.AddInt64(&vc, -1)
}

func (c counter) Match(bit int) bool {
	vc := int64(c)
	return int(atomic.LoadInt64(&vc)) == bit
}

type redbell struct{ c counter }

func (r *redbell) Fire(context interface{}, params map[string]string, payload interface{}) {
	fmt.Printf("Context[%#v] : Params[%#v] : Payload[%#v] : Red bell just rang\n", context, params, payload)
	r.c.Done()
}

type redblackbell struct{ c counter }

func (r *redblackbell) Fire(context interface{}, params map[string]string, payload interface{}) {
	fmt.Printf("Context[%#v] : Params[%#v] : Payload[%#v] : RedBlack bell just rang\n", context, params, payload)
	r.c.Done()
}

type blackbell struct{ c counter }

func (b *blackbell) Fire(context interface{}, params map[string]string, payload interface{}) {
	fmt.Printf("Context[%#v] : Params[%#v] : Payload[%#v] : Black bell just rang\n", context, params, payload)
	b.c.Done()
}

func TestStrictRoutes(t *testing.T) {
	alarm := routes.New(true)

	var c counter

	alarm.MustRegister([]byte(`alarm.red`), &redbell{c})
	alarm.MustRegister([]byte(`alarm.[redish]`), &redbell{c})
	alarm.MustRegister([]byte(`alarm.re*.black`), &redblackbell{c})
	alarm.MustRegister([]byte(`alarm.^re.black`), &redblackbell{c})
	alarm.MustRegister([]byte(`alarm.ck^.black`), &blackbell{c})
	alarm.MustRegister([]byte(`alarm.[/^black$/]`), &blackbell{c})

	fmt.Printf("Details: %#v\n", alarm)

	c.Add()
	path := routes.PathToByte("/alarm/red")
	if err := alarm.Handle(context, path, "Balls"); err != nil {
		fatalFailed(t, "Should have successfully matched path %+s: %s", path, err)
	}
	logPassed(t, "Should have successfully matched path %+s: %s", path)

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %d", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %d", c)

	c.Add()
	path = routes.PathToByte("/alarm/redishblack")
	if err := alarm.Handle(context, path, "Balls"); err != nil {
		fatalFailed(t, "Should have successfully matched path %+s: %s", path, err)
	}
	logPassed(t, "Should have successfully matched path %+s: %s", path, err)

	if !c.Match(0) {
		fatalFailed(t, "Should have successfully reduce counter to 0: %d", c)
	}
	logPassed(t, "Should have successfully reduce counter to 0: %d", c)

	c.Add()
	path = routes.PathToByte("/alarm/redish/black")
	if err := alarm.Handle(context, path, "Balls"); err != nil {
		fatalFailed(t, "Should have successfully matched path %+s: %s", path, err)
	}
	logPassed(t, "Should have successfully matched path %+s: %s", path, err)

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
