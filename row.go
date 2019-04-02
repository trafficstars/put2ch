package put2ch

import (
	"sync"
)

type GetRower interface {
	GetRow() *Row
}

type Row struct {
	tableName string
	columns   []string
	values    []interface{}
}
type Rows []*Row

var (
	rowPool = sync.Pool{New: func() interface{} {
		return &Row{}
	}}
)

func NewRow() *Row {
	return rowPool.Get().(*Row)
}

func (s *Rows) Release() {
	for _, row := range *s {
		row.Release()
	}
	(*s) = (*s)[:0]
}

func (row *Row) Release() {
	row.reset()
	rowPool.Put(row)
}

func (row *Row) reset() {
	row.tableName = ``
	row.columns = row.columns[:0]
	row.values = row.values[:0]
}

func (row *Row) GetTableName() string {
	return row.tableName
}

func (row *Row) GetColumn() []string {
	return row.columns
}

func (row *Row) GetValues() []interface{} {
	return row.values
}
