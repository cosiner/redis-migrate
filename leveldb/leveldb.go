package redis_migrate

import (
	"github.com/cosiner/redis_migrate"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type levelDB struct {
	db *leveldb.DB
}

func NewLevelDB(db *leveldb.DB) (redis_migrate.KeyValueDB, error) {
	return levelDB{
		db: db,
	}, nil
}

func (l levelDB) Close() error {
	return l.db.Close()
}

type levelDBIter struct {
	iter iterator.Iterator
}

func (l levelDBIter) Next() (string, []byte, error) {
	if !l.iter.Next() {
		return "", nil, nil
	}
	return string(l.iter.Key()), l.iter.Value(), nil
}

func (l levelDBIter) Close() error {
	l.iter.Release()
	return nil
}

func (l levelDB) Iterator() redis_migrate.KeyValueIterator {
	return levelDBIter{
		iter: l.db.NewIterator(nil, nil),
	}
}
