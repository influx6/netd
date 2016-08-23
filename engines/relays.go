package engines

// Relays is a type of netd Handler which typically works like a distributed
// PubSub server, where it allows messages based on matching criterias to be
// matched against clients listening for specific criteria.
// Clusters in relay are simply distinct versions of a relay Handler running
// on another endpoint be it locally or remotely and recieve broadcasts and
// share subscriptions critera lists. This allows clusters on remote hosts
// to share/publish to distributed hosts, more over, it allows clusters that
// maybe dying off to shift connections to another hosts while re-spawning
// themselves.
type Relay struct {
}