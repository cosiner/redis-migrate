package migrate

import (
	"regexp"
)

type SourceKeyType string

const (
	SourceKeyTypeSkip   SourceKeyType = ""
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

type keyPatternIterator struct {
	SourceKeyIterator
	s *keyPatternSource
}

type keyPatternFilteredKey struct {
	SourceKey

	s *keyPatternSource
}

func (k keyPatternFilteredKey) Type() (SourceKeyType, error) {
	t, err := k.SourceKey.Type()
	if err != nil || t == SourceKeyTypeSkip {
		return t, err
	}

	key := k.Key()
	if len(k.s.excludes) > 0 {
		for _, e := range k.s.excludes {
			if e.MatchString(key) {
				return SourceKeyTypeSkip, nil
			}
		}
	}
	if len(k.s.includes) > 0 {
		for _, i := range k.s.includes {
			if i.MatchString(key) {
				return t, nil
			}
		}
		return SourceKeyTypeSkip, nil
	}
	return t, nil
}

func (k keyPatternIterator) Next() (SourceKey, error) {
	key, err := k.SourceKeyIterator.Next()
	if key == nil || err != nil {
		return key, err
	}
	return keyPatternFilteredKey{
		SourceKey: key,
		s:         k.s,
	}, nil
}

type keyPatternSource struct {
	includes []*regexp.Regexp
	excludes []*regexp.Regexp
	Source
}

func NewKeyPatternSource(source Source, includes, excludes []string) (Source, error) {
	if len(includes) == 0 && len(excludes) == 0 {
		return source, nil
	}
	parseRegexps := func(regexps []string) ([]*regexp.Regexp, error) {
		rs := make([]*regexp.Regexp, 0, len(regexps))
		for _, s := range regexps {
			r, err := regexp.Compile(s)
			if err != nil {
				return nil, err
			}
			rs = append(rs, r)
		}
		return rs, nil
	}
	k := keyPatternSource{
		Source: source,
	}
	var err error
	k.includes, err = parseRegexps(includes)
	if err != nil {
		return nil, err
	}
	k.excludes, err = parseRegexps(excludes)
	if err != nil {
		return nil, err
	}
	return &k, nil
}

func (s *keyPatternSource) Iterator() SourceKeyIterator {
	return keyPatternIterator{
		SourceKeyIterator: s.Source.Iterator(),
		s:                 s,
	}
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
