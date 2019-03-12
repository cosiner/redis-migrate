package redis_migrate

import (
	"fmt"
)

type Destination interface {
	Close() error

	Set(k string, v []byte) error
	HSet(h, k string, v []byte) error
	SAdd(s, k string) error
	ZAdd(z, k string, s float64) error
	LPush(l, k string) error
}

type prefixedDestination struct {
	s      Destination
	prefix string
}

func NewPrefixedDestination(prefix string, store Destination) (Destination, error) {
	if prefix == "" {
		return nil, fmt.Errorf("invalid store prefix")
	}
	return &prefixedDestination{
		s:      store,
		prefix: prefix,
	}, nil
}

func (ps *prefixedDestination) Close() error { return ps.s.Close() }

func (ps *prefixedDestination) prefixedKey(k string) string {
	return ps.prefix + k
}

func (ps *prefixedDestination) Set(k string, v []byte) error {
	return ps.s.Set(ps.prefixedKey(k), v)
}

func (ps *prefixedDestination) HSet(h, k string, v []byte) error {
	return ps.s.HSet(ps.prefixedKey(h), k, v)
}

func (ps *prefixedDestination) SAdd(s, k string) error {
	return ps.s.SAdd(ps.prefixedKey(s), k)
}

func (ps *prefixedDestination) ZAdd(z, k string, s float64) error {
	return ps.s.ZAdd(ps.prefixedKey(z), k, s)
}

func (ps *prefixedDestination) LPush(l, k string) error {
	return ps.s.LPush(ps.prefixedKey(l), k)
}
