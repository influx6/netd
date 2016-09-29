package netd

import (
	"bytes"
	"errors"
)

//================================================================================================

// behaviourAction defines a core structure for storing actions for giving
// action name.
type delegateAction struct {
	actionName []byte
	action     ActionHandle
}

// Match checks if the giving name matches the action name.
func (ba delegateAction) Match(item []byte) bool {
	return bytes.Equal(item, ba.actionName)
}

// Handle calls the internal actionHandler returning its returned value.
func (ba delegateAction) Handle(context interface{}, data Message, cx *Connection) ([]byte, bool, error) {
	return ba.action(context, data, cx)
}

//==============================================================================

// MessageMorpher transformers the recieved message into another. This allows a interesting
// step of prunning the first item in the received message as the command for the returned message.
type MessageMorpher func(Message) Message

// NextCommand defines a basic message morpher that uses the first item in a message data
// as the returned Message.Command field, else if no data returning the passed in message.
func NextCommand(msg Message) Message {
	if len(msg.Data) != 0 {
		return Message{
			Command: msg.Data[0],
			Data:    msg.Data[1:],
		}
	}

	return msg
}

//==============================================================================

// Action defines the interface for action providers  which provide match and handle functions for request.
type Action interface {
	Match([]byte) bool
	Handle(context interface{}, msg Message, cx *Connection) ([]byte, bool, error)
}

// ActionHandle defines the function handler expected to provide action handlers for
// document requests.
type ActionHandle func(context interface{}, data Message, cx *Connection) ([]byte, bool, error)

// Delegation defines a interface which allows the creation of transactions on
// incoming data. Behaviours are strict in that for a series of linked actions
// the paths that leads to them are unique and can not diverge into other behaviours.
type Delegation interface {
	With(...Action) Delegation
	Next(actionName string, d Delegation) Delegation
	Action(actionName string, action ActionHandle) Delegation
	Delegate(actionName string, d Delegation, m ...MessageMorpher) Delegation
	Handle(context interface{}, msg Message, cx *Connection) ([]byte, bool, error)
}

// NewDelegation returns a new instance of a structure implementing the Delegation interface.
func NewDelegation() Delegation {
	var d delegate
	return &d
}

// WrapAsAction returns a Action which wrapps a delegation allow one delegation to call another.
func WrapAsAction(actionName string, d Delegation, morpher MessageMorpher) Action {
	return delegateAction{
		actionName: []byte(actionName),
		action: func(ctx interface{}, msg Message, cx *Connection) ([]byte, bool, error) {
			if morpher != nil {
				return d.Handle(ctx, morpher(msg), cx)
			}

			return d.Handle(ctx, msg, cx)
		},
	}
}

type delegate struct {
	actions []Action
}

// Next calls the Delegation.Delegate method but provides the NextCommand as a message morpher.
// It provides a convenience method of morphing the command of the passed in message which is then
// passed to the provided delegation's Handle method.
func (bh *delegate) Next(actionName string, delegation Delegation) Delegation {
	return bh.Delegate(actionName, delegation, NextCommand)
}

// Delegate adds another delegation into the action chain through the giving action name.
// This allows calling other delegations within another depending on the arrival of its action name.
// NOTE: Though a variadic list is used for the MessageMorpher only the first is used/picked, this is done
// for convenience purposes.
func (bh *delegate) Delegate(actionName string, delegation Delegation, mo ...MessageMorpher) Delegation {
	if mo != nil {
		return bh.With(WrapAsAction(actionName, delegation, mo[0]))
	}

	return bh.With(WrapAsAction(actionName, delegation, nil))
}

// Action adds the giving action name and handler into the delegate action lists.
func (bh *delegate) Action(actionName string, handler ActionHandle) Delegation {
	bh.With(delegateAction{
		actionName: []byte(actionName),
		action:     handler,
	})

	return bh
}

// With creates a next connect to the provided behaviour if this behaviour
// has no attached action else panics.
func (bh *delegate) With(actions ...Action) Delegation {
	bh.actions = append(bh.actions, actions...)
	return bh
}

// ErrActionNotFound is returned when a giving delegate finds no attached action.
var ErrActionNotFound = errors.New("Action Not Found")

// Handle calls the internal Action.Handle function for the given function which matches the needed Handler.
func (bh *delegate) Handle(context interface{}, data Message, cx *Connection) ([]byte, bool, error) {
	for _, action := range bh.actions {
		if action.Match(data.Command) {
			return action.Handle(context, data, cx)
		}
	}

	return nil, true, ErrActionNotFound
}

//================================================================================================
