package redis

import (
	"fmt"

	"github.com/cosiner/redis_migrate"
	"github.com/go-redis/redis"
)

type serverDestination struct {
	client *redis.Client
}

func NewServerDestination(client *redis.Client) (redis_migrate.Destination, error) {
	return serverDestination{client: client}, nil
}

func (ss serverDestination) Close() error {
	return ss.client.Close()
}

func (ss serverDestination) Set(k string, v []byte) error {
	_, err := ss.client.Set(k, v, 0).Result()
	return err
}

func (ss serverDestination) HSet(h, k string, v []byte) error {
	_, err := ss.client.HSet(h, k, v).Result()
	return err
}

func (ss serverDestination) SAdd(s, k string) error {
	_, err := ss.client.SAdd(s, k).Result()
	return err
}

func (ss serverDestination) ZAdd(z, k string, s float64) error {
	_, err := ss.client.ZAdd(z, redis.Z{Score: s, Member: k}).Result()
	return err
}

func (ss serverDestination) LPush(l, k string) error {
	_, err := ss.client.LPush(l, k).Result()
	return err
}

type serverSource struct {
	client *redis.Client
}

func NewServerSource(client *redis.Client) (redis_migrate.Source, error) {
	return serverSource{
		client: client,
	}, nil
}

type serverSourceKey struct {
	s   *redis.Client
	key string
}

func (s serverSourceKey) Key() string {
	return s.key
}

func (s serverSourceKey) Type() (redis_migrate.SourceKeyType, error) {
	typ, err := s.s.Type(s.key).Result()
	if err != nil {
		return "", err
	}
	switch typ {
	case "string":
		return redis_migrate.SourceKeyTypeString, nil
	case "list":
		return redis_migrate.SourceKeyTypeList, nil
	case "hash":
		return redis_migrate.SourceKeyTypeHash, nil
	case "set":
		return redis_migrate.SourceKeyTypeSet, nil
	case "zset":
		return redis_migrate.SourceKeyTypeZSet, nil
	default:
		return "", fmt.Errorf("unsupported key type %s, %s", s.key, typ)
	}
}

func (ss serverSource) Close() error { return ss.client.Close() }

type serverKeyIterator struct {
	s   *redis.Client
	err error

	cursor   uint64
	finished bool

	keys []redis_migrate.SourceKey
	idx  int
}

func (ss *serverKeyIterator) fetch() {
	if ss.err != nil {
		return
	}

	const BUFSIZE = 1024
	keys, cursor, err := ss.s.Scan(ss.cursor, "*", BUFSIZE).Result()
	if err != nil {
		ss.finished = true
		ss.err = err
		return
	}

	if len(keys) > 0 {
		if cap(ss.keys) == 0 {
			ss.keys = make([]redis_migrate.SourceKey, 0, BUFSIZE)
		}
	}

	ss.keys = ss.keys[:0]
	ss.idx = 0
	for _, key := range keys {
		ss.keys = append(ss.keys, serverSourceKey{s: ss.s, key: key})
	}
	ss.cursor = cursor
	ss.finished = cursor <= 0 || len(keys) == 0
	return
}

func (ss *serverKeyIterator) Error() error {
	return ss.err
}

func (ss *serverKeyIterator) Next() (redis_migrate.SourceKey, error) {
	if !ss.finished && ss.idx >= len(ss.keys) {
		ss.fetch()
	}
	if ss.idx >= len(ss.keys) {
		return nil, nil
	}
	k := ss.keys[ss.idx]
	ss.idx++
	return k, nil
}

func (ss *serverKeyIterator) Close() error {
	return nil
}

func (ss serverSource) Iterator() redis_migrate.SourceKeyIterator {
	return &serverKeyIterator{
		s:      ss.client,
		cursor: 0,
	}
}

func (ss serverSource) allowNil(err error) error {
	if err == redis.Nil {
		return nil
	}
	return err
}

func (ss serverSource) Get(k redis_migrate.SourceKey) ([]byte, error) {
	val, err := ss.client.Get(k.Key()).Result()
	return []byte(val), ss.allowNil(err)
}

func (ss serverSource) HItems(k redis_migrate.SourceKey) ([]redis_migrate.SourceHashItem, error) {
	kvs, err := ss.client.HGetAll(k.Key()).Result()
	err = ss.allowNil(err)
	if err != nil {
		return nil, err
	}

	items := make([]redis_migrate.SourceHashItem, 0, len(kvs))
	for k, v := range kvs {
		items = append(items, redis_migrate.SourceHashItem{Key: k, Value: []byte(v)})
	}
	return items, nil
}

func (ss serverSource) LItems(k redis_migrate.SourceKey) ([]string, error) {
	keys, err := ss.client.LRange(k.Key(), 0, -1).Result()
	err = ss.allowNil(err)
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (ss serverSource) SMembers(k redis_migrate.SourceKey) ([]string, error) {
	keys, err := ss.client.SMembers(k.Key()).Result()
	err = ss.allowNil(err)
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (ss serverSource) ZMembers(k redis_migrate.SourceKey) ([]redis_migrate.SourceZSetMember, error) {
	items, err := ss.client.ZRangeWithScores(k.Key(), 0, -1).Result()
	err = ss.allowNil(err)
	if err != nil {
		return nil, err
	}
	members := make([]redis_migrate.SourceZSetMember, 0, len(items))
	for _, item := range items {
		members = append(members, redis_migrate.SourceZSetMember{
			Score: item.Score,
			Key:   fmt.Sprint(item.Member),
		})
	}

	return members, nil
}

func NewRedisClient(addr, password string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Network:  "tcp",
		Addr:     addr,
		Password: password,
	})
	_, err := client.Ping().Result()
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}
