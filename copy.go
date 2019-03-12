package redis_migrate

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type CopyRecorder interface {
	Error(message string, err error, kvs ...interface{})

	Finish()
	Key(typ SourceKeyType, key string)
}

type stdCopyRecorder struct {
	buf bytes.Buffer
	out io.Writer
}

func NewStdCopyRecorder(out io.Writer) CopyRecorder {
	if out == nil {
		out = os.Stdout
	}
	return &stdCopyRecorder{
		out: out,
	}
}

func (c *stdCopyRecorder) flush() {
	_, _ = c.buf.WriteTo(c.out)
	c.buf.Reset()
}

func (c *stdCopyRecorder) Error(message string, err error, kvs ...interface{}) {
	c.buf.Reset()
	c.buf.WriteString(message)
	c.buf.WriteString(": ")
	c.buf.WriteString(err.Error())
	for i := 0; i+1 < len(kvs); i += 2 {
		c.buf.WriteString(", ")
		c.buf.WriteString(fmt.Sprint(kvs[i]))
		c.buf.WriteString(": ")
		c.buf.WriteString(fmt.Sprint(kvs[i+1]))
	}

	c.buf.WriteString("\n")
	c.flush()
}

func (c *stdCopyRecorder) Finish() {
	c.buf.WriteString("copy finished.\n")
	c.flush()
}

func (c *stdCopyRecorder) Key(typ SourceKeyType, key string) {
	_, _ = fmt.Fprintf(&c.buf, "start: %s: %s\n", typ, key)
	c.flush()
}

func Copy(src Source, dst Destination, recorder CopyRecorder) {
	iter := src.Iterator()
	defer func() {
		err := iter.Close()
		if err != nil {
			recorder.Error("close source iterator failed", err)
		}
	}()

	for {
		key, err := iter.Next()
		if err != nil {
			recorder.Error("iterate next key failed", err)
			continue
		}
		if key == nil {
			break
		}

		typ, err := key.Type()
		if err != nil {
			recorder.Error("retrieve key type failed", err, "key", key.Key())
			continue
		}

		recorder.Key(typ, key.Key())
		switch typ {
		default:
			recorder.Error("unsupported key type", err, "type", typ, "key", key.Key())
		case SourceKeyTypeString:
			val, err := src.Get(key)
			if err != nil {
				recorder.Error("get string key value failed", err, "type", typ, "key", key.Key())
			} else {
				err = dst.Set(key.Key(), val)
				if err != nil {
					recorder.Error("set string key value failed", err, "type", typ, "key", key.Key())
				}
			}
		case SourceKeyTypeHash:
			items, err := src.HItems(key)
			if err != nil {
				recorder.Error("get hash items failed", err, "type", typ, "key", key.Key())
			} else {
				for _, kv := range items {
					err = dst.HSet(key.Key(), kv.Key, kv.Value)
					if err != nil {
						recorder.Error("set hash item failed", err, "type", typ, "key", key.Key(), "field", kv.Key)
					}
				}
			}
		case SourceKeyTypeList:
			items, err := src.LItems(key)
			if err != nil {
				recorder.Error("get list items failed", err, "type", typ, "key", key.Key())
			} else {
				for _, item := range items {
					err = dst.LPush(key.Key(), item)
					if err != nil {
						recorder.Error("push list item failed", err, "type", typ, "key", key.Key(), "item", item)
					}
				}
			}
		case SourceKeyTypeSet:
			members, err := src.SMembers(key)
			if err != nil {
				recorder.Error("get set members failed", err, "type", typ, "key", key.Key())
			} else {
				for _, member := range members {
					err = dst.SAdd(key.Key(), member)
					if err != nil {
						recorder.Error("add set member failed", err, "type", typ, "key", key.Key(), "member", member)
					}
				}
			}
		case SourceKeyTypeZSet:
			members, err := src.ZMembers(key)
			if err != nil {
				recorder.Error("get zset members failed", err, "type", typ, "key", key.Key())
			} else {
				for _, member := range members {
					err = dst.ZAdd(key.Key(), member.Key, member.Score)
					if err != nil {
						recorder.Error("add zset member failed", err, "type", typ, "key", key.Key(), "member", member)
					}
				}
			}
		}
	}
	err := iter.Error()
	if err != nil {
		recorder.Error("iterator errors", err)
	}
	recorder.Finish()
}
