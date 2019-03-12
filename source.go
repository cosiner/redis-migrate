package redis_migrate

type SourceKeyType string

const (
	SourceKeyTypeString SourceKeyType = "string"
	SourceKeyTypeHash   SourceKeyType = "hash"
	SourceKeyTypeList   SourceKeyType = "list"
	SourceKeyTypeSet    SourceKeyType = "set"
	SourceKeyTypeZSet   SourceKeyType = "zset"
)

type SourceKey interface {
	Key() string
	Type() (SourceKeyType, error)
}

type SourceKeyIterator interface {
	Error() error
	Next() (SourceKey, error)
	Close() error
}

type SourceHashItem struct {
	Key   string
	Value []byte
}

type SourceZSetMember struct {
	Key   string
	Score float64
}

type Source interface {
	Close() error
	Iterator() SourceKeyIterator

	Get(k SourceKey) ([]byte, error)
	HItems(k SourceKey) ([]SourceHashItem, error)
	LItems(k SourceKey) ([]string, error)
	SMembers(k SourceKey) ([]string, error)
	ZMembers(k SourceKey) ([]SourceZSetMember, error)
}

type KeyValueIterator interface {
	Next() (key string, value []byte, err error)
	Close() error
}

type KeyValueDB interface {
	Close() error

	Iterator() KeyValueIterator
}

type KeyValueItem struct {
	Key  string
	Type SourceKeyType

	StringValue []byte

	HashField      string
	HashFieldValue []byte

	ListItem string

	SetMember string

	ZSetMember string
	ZSetScore  float64
}

type SourceKeyParser interface {
	Parse(key string, val []byte) (KeyValueItem, error)
}

type keyValueSource struct {
	db     KeyValueDB
	parser SourceKeyParser
}

func NewKeyValueSource(db KeyValueDB, parser SourceKeyParser) Source {
	return keyValueSource{
		db:     db,
		parser: parser,
	}
}

func (s keyValueSource) Close() error {
	return s.db.Close()
}

type keyValueSourceKey struct {
	item KeyValueItem
}

func (k keyValueSourceKey) Key() string {
	return k.item.Key
}

func (k keyValueSourceKey) Type() (SourceKeyType, error) {
	return k.item.Type, nil
}

type keyValueIterator struct {
	iter   KeyValueIterator
	parser SourceKeyParser
	err    error
}

func (k *keyValueIterator) Close() error {
	return k.iter.Close()
}

func (k *keyValueIterator) Error() error {
	return k.err
}

func (k *keyValueIterator) Next() (SourceKey, error) {
	if k.err != nil {
		return nil, nil
	}

	key, val, err := k.iter.Next()
	if err != nil {
		k.err = err
		return nil, nil
	}
	if key == "" {
		return nil, nil
	}

	item, err := k.parser.Parse(key, val)
	if err != nil {
		return nil, err
	}
	return keyValueSourceKey{
		item: item,
	}, nil
}

func (s keyValueSource) Iterator() SourceKeyIterator {
	iter := s.db.Iterator()
	return &keyValueIterator{
		iter:   iter,
		parser: s.parser,
	}
}

func (s keyValueSource) Get(k SourceKey) ([]byte, error) {
	item, _ := k.(keyValueSourceKey)
	return item.item.StringValue, nil
}

func (s keyValueSource) HItems(k SourceKey) ([]SourceHashItem, error) {
	item, _ := k.(keyValueSourceKey)
	return []SourceHashItem{
		{Key: item.item.HashField, Value: item.item.HashFieldValue},
	}, nil
}

func (s keyValueSource) LItems(k SourceKey) ([]string, error) {
	item, _ := k.(keyValueSourceKey)
	return []string{item.item.ListItem}, nil
}

func (s keyValueSource) SMembers(k SourceKey) ([]string, error) {
	item, _ := k.(keyValueSourceKey)
	return []string{item.item.SetMember}, nil
}

func (s keyValueSource) ZMembers(k SourceKey) ([]SourceZSetMember, error) {
	item, _ := k.(keyValueSourceKey)
	return []SourceZSetMember{
		{Key: item.item.ZSetMember, Score: item.item.ZSetScore},
	}, nil
}
