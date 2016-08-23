package routes

import (
	"bytes"
	"errors"
	"regexp"
	"runtime"
	"sync"
)

var (
	sbl               = "."
	slash             = byte('/')
	slashSlice        = []byte{slash}
	colon             = byte(':')
	colonSlice        = []byte{colon}
	emptySlice        = []byte("")
	sublist           = byte('.')
	sublistSlice      = []byte{sublist}
	edges             = byte('^')
	edgesSlice        = []byte{edges}
	contains          = byte('*')
	containsSlice     = []byte{contains}
	startBracket      = byte('[')
	startBracketSlice = []byte{startBracket}
	endBracket        = byte(']')
	endBracketSlice   = []byte{endBracket}
)

// Trace defines an interface which receives data trace data logs.
type Trace interface {
	Trace(context interface{}, msg []byte)
}

//==============================================================================

// Subscriber defines an interface for routes to be fired upon when matched.
type Subscriber interface {
	Fire(context interface{}, params map[string]string, payload interface{})
}

// Subscription defines a struct for storing subscriptions.
type Subscription struct {
	root *level
}

// New returns a new Subscription which can be used to route to specific
// subscribers. The variadic argument is used for elegance and to allow zero,
// zero value tracers.
func New(strict bool, t ...Trace) *Subscription {
	var sub Subscription

	var tr Trace

	if len(t) > 0 {
		tr = t[0]
	}

	sub.root = newLevel(strict, tr)

	return &sub
}

// Handle calls the giving path slice, if found and applies the payload else
// returns an error.
func (s *Subscription) Handle(context interface{}, path []byte, payload interface{}) error {
	return s.root.Resolve(context, path, payload)
}

// Handle calls the giving path, if found and applies the payload else
// returns an error.
func (s *Subscription) HandlePath(context interface{}, path string, payload interface{}) error {
	return s.root.Resolve(context, PathToByte(path), payload)
}

// MustHandle calls the given path slice, if found and applies the payload else
// panics.
func (s *Subscription) MustHandle(context interface{}, path []byte, payload interface{}) {
	if err := s.root.Resolve(context, path, payload); err != nil {
		panic(err)
	}
}

// Register adds the new giving path slice into the subscription for routing.
func (s *Subscription) Register(path []byte, sub Subscriber) error {
	return s.root.Add(path, sub)
}

// MustRegister adds the new giving path slice into the subscription for routing
// else panics.
func (s *Subscription) MustRegister(path []byte, sub Subscriber) {
	if err := s.root.Add(path, sub); err != nil {
		panic(err)
	}
}

// RegisterPath adds the new giving path into the subscription for routing.
func (s *Subscription) RegisterPath(path string, sub Subscriber) error {
	return s.root.Add([]byte(path), sub)
}

// Unregister removes the existing giving path slice into the subscription for routing.
func (s *Subscription) Unregister(path []byte, sub Subscriber) error {
	return s.root.Remove(path, sub)
}

// UnregisterPath removes the existing giving path into the subscription for routing.
func (s *Subscription) UnregisterPath(path string, sub Subscriber) error {
	return s.root.Remove([]byte(path), sub)
}

// MustUnregister removes the existing giving path slice into the subscription for routing
// else panics.
func (s *Subscription) MustUnregister(path []byte, sub Subscriber) {
	if err := s.root.Remove(path, sub); err != nil {
		panic(err)
	}
}

//==============================================================================

type level struct {
	rw         sync.RWMutex
	strictPath bool
	tracer     Trace
	all        *node
	nodes      map[string]*node
}

func newLevel(strictPath bool, tracer Trace) *level {
	return &level{
		strictPath: strictPath,
		tracer:     tracer,
		nodes:      make(map[string]*node),
		all: &node{
			sid:     containsSlice[0:],
			matcher: func(b []byte) bool { return true },
		},
	}
}

type node struct {
	next    *level
	sid     []byte
	ns      []byte
	subs    []Subscriber
	matcher func([]byte) bool
}

// Size returns the total number of nodes on this level.
func (s *level) Size() int {
	s.rw.RLock()
	n := len(s.nodes)
	s.rw.RUnlock()
	return n
}

// Resolve checks if the giving path is a match within the giving level's routes.
func (s *level) Resolve(context interface{}, pattern []byte, payload interface{}) error {
	pLen := len(pattern)

	tracer := s.tracer
	params := make(map[string]string)

	if pLen == 1 && pattern[0] == contains {
		s.rw.RLock()
		{
			for _, sub := range s.all.subs {
				recovers(context, func() {
					sub.Fire(context, params, payload)
				}, tracer)
			}
		}
		s.rw.RUnlock()

		return nil
	}

	tokens, err := splitResolveToken(pattern)
	if err != nil {
		return err
	}

	return s.resolve(context, nil, tokens, params, payload)
}

func (s *level) resolve(context interface{}, node *node, tokens [][]byte, params map[string]string, payload interface{}) error {
	tLen := len(tokens)

	s.rw.RLock()
	tracer := s.tracer
	strict := s.strictPath
	s.rw.RUnlock()

	if tLen == 0 && node == nil {
		return errors.New("No route found for given path")
	}

	if tLen == 0 {
		s.rw.RLock()
		{
			for _, sub := range node.subs {
				recovers(context, func() {
					sub.Fire(context, params, payload)
				}, tracer)
			}
		}
		s.rw.RUnlock()

		return nil
	}

	token := tokens[0]

	if tLen == 1 {
		tokens = tokens[:0]
	} else {
		tokens = tokens[1:]
	}

	var ok bool

	if node == nil {

		s.rw.RLock()
		node, ok = s.nodes[string(token)]
		s.rw.RUnlock()

		if ok && !node.matcher(token) {

			s.rw.RLock()
			{
				for _, node = range s.nodes {
					if !node.matcher(token) {
						continue
					}
				}
			}
			s.rw.RUnlock()

		}

		if node == nil {
			return errors.New("No route matches path")
		}
	}

	if ok && !node.matcher(token) {
		return errors.New("Path fails to match route")
	}

	params[string(node.ns)] = string(token)

	s.rw.RLock()
	{
		for _, sub := range s.all.subs {
			recovers(context, func() {
				sub.Fire(context, params, payload)
			}, tracer)
		}
	}
	s.rw.RUnlock()

	if strict && s.Size() == 0 && tLen > 0 {
		s.rw.RLock()
		{
			for _, inode := range s.nodes {
				if inode == node {
					continue
				}

				if !inode.matcher(token) {
					continue
				}

				node = inode
				break
			}
		}
		s.rw.RUnlock()
	}

	return node.next.resolve(context, node, tokens, params, payload)
}

// Add adds a new subscriber into the subscription list with the provided pattern.
func (s *level) Add(pattern []byte, subscriber Subscriber) error {
	pLen := len(pattern)

	if pLen == 1 && pattern[0] == contains {
		s.all.subs = append(s.all.subs, subscriber)
		return nil
	}

	tokens := splitToken(pattern)
	return s.add(tokens, subscriber)
}

func (s *level) add(patterns [][]byte, subscriber Subscriber) error {
	var strictPath bool

	s.rw.RLock()
	strictPath = s.strictPath
	s.rw.RUnlock()

	pLen := len(patterns)

	for i := 0; i < len(patterns); i++ {
		item := patterns[i]
		itemLen := len(item)

		var match func([]byte) bool

		if bytes.Contains(item, edgesSlice) {
			if itemLen == 1 {
				return errors.New("Invalid Token usage, Edges('^') must be used at start or end of section")
			}

			if bytes.HasPrefix(item, edgesSlice) {
				item = item[1:]

				match = func(d []byte) bool {
					if bytes.HasPrefix(d, item) {
						return true
					}

					return false
				}

				continue
			}

			if bytes.HasPrefix(item, edgesSlice) {
				item = item[:itemLen-1]

				match = func(d []byte) bool {
					if bytes.HasSuffix(d, item) {
						return true
					}

					return false
				}

				continue
			}

			s.rw.RLock()
			nodeItem, ok := s.nodes[string(item)]
			s.rw.RUnlock()
			if !ok {
				nodeItem = &node{
					next:    newLevel(strictPath, s.tracer),
					sid:     item,
					ns:      nsToken(item),
					matcher: match,
				}

				s.rw.Lock()
				s.nodes[string(item)] = nodeItem
				s.rw.Unlock()
			}

			if i+1 >= pLen {
				s.rw.Lock()
				nodeItem.subs = append(nodeItem.subs, subscriber)
				s.rw.Unlock()
				return nil
			}

			return nodeItem.next.add(patterns[i+1:], subscriber)
		}

		if bytes.Contains(item, containsSlice) {
			if itemLen == 1 {
				s.all.subs = append(s.all.subs, subscriber)
				return nil
			}

			item = bytes.Replace(item, containsSlice, emptySlice, 1)
			match = func(d []byte) bool {
				if bytes.Contains(d, item) {
					return true
				}

				return false
			}

			s.rw.RLock()
			nodeItem, ok := s.nodes[string(item)]
			s.rw.RUnlock()
			if !ok {
				nodeItem = &node{
					next:    newLevel(strictPath, s.tracer),
					sid:     item,
					ns:      nsToken(item),
					matcher: match,
				}

				s.rw.Lock()
				s.nodes[string(item)] = nodeItem
				s.rw.Unlock()
			}

			if i+1 >= pLen {
				s.rw.Lock()
				nodeItem.subs = append(nodeItem.subs, subscriber)
				s.rw.Unlock()
				return nil
			}

			return nodeItem.next.add(patterns[i+1:], subscriber)
		}

		if bytes.Contains(item, startBracketSlice) && bytes.Contains(item, endBracketSlice) {
			word, regex, before := yankRegExp(item)

			wordLen := len(word)
			matchex := regexp.MustCompile(string(regex))

			if before {
				match = func(d []byte) bool {
					if wordLen != 0 && !bytes.HasPrefix(d, word) {
						return false
					}

					if wordLen != 0 {
						d = d[wordLen:]
					}

					if !matchex.Match(d) {
						return false
					}

					return true
				}
			} else {
				match = func(d []byte) bool {
					if wordLen != 0 && !bytes.HasSuffix(d, word) {
						return false
					}

					if wordLen != 0 {
						d = d[:wordLen]
					}

					if !matchex.Match(d) {
						return false
					}

					return true
				}
			}

			s.rw.RLock()
			nodeItem, ok := s.nodes[string(item)]
			s.rw.RUnlock()
			if !ok {
				nodeItem = &node{
					next:    newLevel(strictPath, s.tracer),
					sid:     item,
					ns:      nsToken(word),
					matcher: match,
				}

				s.rw.Lock()
				s.nodes[string(item)] = nodeItem
				s.rw.Unlock()
			}

			if i+1 >= pLen {
				s.rw.Lock()
				nodeItem.subs = append(nodeItem.subs, subscriber)
				s.rw.Unlock()
				return nil
			}

			return nodeItem.next.add(patterns[i+1:], subscriber)
		}

		match = func(d []byte) bool {
			if bytes.Equal(d, item) {
				return true
			}

			return false
		}

		s.rw.RLock()
		nodeItem, ok := s.nodes[string(item)]
		s.rw.RUnlock()
		if !ok {
			nodeItem = &node{
				next:    newLevel(strictPath, s.tracer),
				sid:     item,
				ns:      nsToken(item),
				matcher: match,
			}

			s.rw.Lock()
			s.nodes[string(item)] = nodeItem
			s.rw.Unlock()
		}

		if i+1 >= pLen {
			s.rw.Lock()
			nodeItem.subs = append(nodeItem.subs, subscriber)
			s.rw.Unlock()
			return nil
		}

		return nodeItem.next.add(patterns[i+1:], subscriber)
	}

	return nil
}

// Remove delets the subscriber from the subscription list with the provided pattern.
func (s *level) Remove(pattern []byte, subscriber Subscriber) error {
	pLen := len(pattern)

	if pLen == 1 && pattern[0] == contains {
		nodeItem := s.all

		s.rw.RLock()
		subs := nodeItem.subs
		subLen := len(nodeItem.subs)
		s.rw.RUnlock()

		for j := 0; j < subLen; j++ {
			if sub := subs[j]; sub == subscriber {
				if subLen == 1 {
					s.rw.Lock()
					nodeItem.subs = subs[:0]
					s.rw.Unlock()
					return nil
				}

				s.rw.Lock()
				nodeItem.subs[j] = subs[subLen-1]
				s.rw.Unlock()
				return nil
			}
		}

		return errors.New("Subscriber not found in registry")
	}

	tokens := splitToken(pattern)
	return s.remove(tokens, subscriber)
}

func (s *level) remove(patterns [][]byte, subscriber Subscriber) error {
	pLen := len(patterns)

	for i := 0; i < len(patterns); i++ {
		item := patterns[i]

		s.rw.RLock()
		nodeItem, ok := s.nodes[string(item)]
		s.rw.RUnlock()

		if !ok {
			return errors.New("Invalid route")
		}

		if i+1 >= pLen {
			s.rw.RLock()
			subs := nodeItem.subs
			s.rw.RUnlock()

			subLen := len(nodeItem.subs)

			for j := 0; j < subLen; j++ {
				if sub := subs[j]; sub == subscriber {
					if subLen == 1 {
						s.rw.Lock()
						nodeItem.subs = subs[:0]
						s.rw.Unlock()
						return nil
					}

					s.rw.Lock()
					nodeItem.subs[j] = subs[subLen-1]
					s.rw.Unlock()
					return nil
				}
			}

			return errors.New("Subscriber not found in registry")
		}

		return nodeItem.next.remove(patterns[i+1:], subscriber)
	}

	return nil
}

// ToByte provides a quick function to transform a path string (`/ded/fr/fg`)
// into a slice of point bytes (`/ded/fr/fg => ded.fr.fg ==> []byte{11,33,...}`).
func PathToByte(path string) []byte {
	tokens := []byte(path)
	tokens = bytes.TrimSpace(tokens)
	tokens = bytes.TrimPrefix(tokens, slashSlice)
	tokens = bytes.TrimSuffix(tokens, slashSlice)
	return bytes.Replace(tokens, slashSlice, sublistSlice, -1)
}

func nsToken(token []byte) []byte {
	return bytes.Join([][]byte{colonSlice, token}, emptySlice)
}

func splitToken(pattern []byte) [][]byte {
	pLen := len(pattern)

	var tokens [][]byte
	var token []byte

	for i := 0; i < pLen; i++ {
		item := pattern[i]

		if item == sublist {
			tokens = append(tokens, token)
			token = nil
			continue
		}

		token = append(token, item)
	}

	return tokens
}

func recovers(context interface{}, fx func(), tracer Trace) {
	defer func() {
		if err := recover(); err != nil {
			if tracer != nil {
				size := 1 << 16
				trace := make([]byte, size)
				trace = trace[:runtime.Stack(trace, true)]
				tracer.Trace(context, trace)
			}
			return
		}
	}()

	fx()
}

func splitResolveToken(pattern []byte) ([][]byte, error) {
	var tokens [][]byte
	var token []byte

	pLen := len(pattern)

	for i := 0; i < pLen; i++ {
		item := pattern[i]

		switch item {
		case colon, edges, contains, startBracket, endBracket:
			return nil, errors.New("Resolve Path cant contain pattern characters(`^`,`>`,`*`,`[`,`]`,`[]`")
		case sublist:
			tokens = append(tokens, token)
			token = nil
			continue
		default:
			token = append(token, item)
		}

	}

	return tokens, nil
}

// yankRegExp splits the giving pattern `id[\d+]`.
func yankRegExp(pattern []byte) ([]byte, []byte, bool) {
	var word, rgu []byte

	var foundReg bool
	var before bool

	for _, item := range pattern {
		if item == startBracket {
			if len(word) != 0 {
				before = true
			}

			foundReg = true
			continue
		}

		if item == endBracket {
			foundReg = false
			continue
		}

		if foundReg {
			rgu = append(rgu, item)
			continue
		}

		word = append(word, item)
	}

	return word, rgu, before
}
