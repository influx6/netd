package core

import (
	"bytes"

	"github.com/influx6/netd"
)

// Action defines the function handler expected to provide action handlers for
// document requests.
type Action func(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error)

// Delegation defines a interface which allows the creation of transactions on
// incoming data. Behaviours are strict in that for a series of linked actions
// the paths that leads to them are unique and can not diverge into other behaviours.
type Delegation interface {
	netd.Middleware
	Action(actionName string, action Action)
	Delegate(name string) Delegation
}

// behaviourAction defines a core structure for storing actions for giving
// action name.
type delegateAction struct {
	ActionName []byte
	Action     Action
}

// Match checks if the giving name matches the action name.
func (ba *delegateAction) Match(item []byte) bool {
	return bytes.Equal(item, ba.ActionName)
}

//==============================================================================

type delegate struct {
	action        Action
	next          Delegation
	behaviourName []byte
}

// Match matches the provided name with the behaviour names.
func (bh *delegate) Match(item []byte) bool {
	return bytes.Equal(item, bh.behaviourName)
}

// Delegate creates a next connect to the provided behaviour if this behaviour
// has no attached action else panics.
func (bh *delegate) Delegate(actionName string) Delegation {
	if bh.next != nil {
		panic("Delegation already has a connected behaviour")
	}

	if bh.action != nil {
		panic("Delegation already has attached action, hence a terminal behaviour")
	}

	var bl delegate
	bl.behaviourName = []byte(actionName)

	bh.next = &bl

	return bh.next
}

func (bh *delegate) Action(actionName string, action Action) {
	if bh.action != nil {
		panic("Delegation[%s] already has a action supplied")
	}

	bh.action = &delegateAction{
		ActionName: []byte(actionName),
		Action:     action,
	}
}

func (bh *delegate) HandleRoute(data [][]byte) {
	if len(data) < 0 {
		return
	}

	block := data[0]
	if bh.Match(block) {
		if bh.next != nil {
			return bh.next.Handle(context, m, cx)
		}
	}
}
