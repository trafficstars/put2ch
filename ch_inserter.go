package put2ch

import (
	"database/sql"

	_ "github.com/kshvakov/clickhouse"
	"github.com/xaionaro-go/errors"
	"github.com/xaionaro-go/spinlock"
)

const (
	defaultBunchSize = 1000
)

type CHInserter struct {
	spinlock.Locker

	RowsChan  chan *Row
	Queue     Rows
	BunchSize uint
	LastError error
	DB        *sql.DB
	Logger    Logger

	sqlStatementGeneratorCache sqlStatementGeneratorCache
}

type sqlStatementGeneratorCache struct {
}

func NewCHInserter(dsn string, rowsChan chan *Row, logger Logger) (*CHInserter, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Wrap(err)
	}
	return &CHInserter{
		DB:        db,
		RowsChan:  rowsChan,
		BunchSize: defaultBunchSize,
		Logger:    logger,
	}, nil
}

func (ch *CHInserter) LogWarning(args ...interface{}) {
	if ch.Logger == nil {
		return
	}

	ch.Logger.Warning(args...)
}

func (ch *CHInserter) Loop() error {
	for {
		ch.Logger.Trace(`L`)
		row := <-ch.RowsChan
		ch.Logger.Trace(`/L`)

		err := ch.PushToQueue(row)
		if err != nil {
			return errors.Wrap(err)
		}

		if ch.GetQueueLength() > ch.BunchSize*2 {
			return ErrTooMuchRowsInQueue.Wrap(ch.LastError)
		}
	}

	return nil
}

func (ch *CHInserter) GetQueueLength() (result uint) {
	ch.LockDo(func() {
		result = uint(len(ch.Queue))
	})
	return
}

func (ch *CHInserter) PushToQueue(rows ...*Row) (err error) {
	ch.LockDo(func() {
		ch.Queue = append(ch.Queue, rows...)
		if uint(len(ch.Queue)) < ch.BunchSize {
			return
		}

		err = ch.Insert(ch.Queue)
		if err == nil {
			ch.Queue.Release()
		}
	})

	return
}

type getSQLStatementStringForRowerT struct {
	/*	spinlock.Locker
		previousTableName string
		previousColumns []string*/
}

var getSQLStatementStringForRower = &getSQLStatementStringForRowerT{}

func (t *getSQLStatementStringForRowerT) GetSQLStatementStringForRow(row *Row) string {
	/*t.Lock()
	previousTableName := t.previousTableName
	previousColumns := t.previousColumns*/

	tableName := row.GetTableName()
	columns := row.GetColumn()

	//if tableName == previousTableName &&

	buf := newBuffer()
	buf.WriteString(`INSERT INTO `)
	buf.WriteString(tableName)
	buf.WriteString(` (`)

	if len(columns) == 0 {
		panic(`len(columns) == 0`)
	}
	buf.WriteString(columns[0])
	for _, column := range columns[1:] {
		buf.WriteRune(',')
		buf.WriteString(column)
	}
	buf.WriteString(`) VALUES (?`)
	for range columns[:1] {
		buf.WriteString(`,?`)
	}
	buf.WriteRune(')')
	r := buf.String()
	buf.Release()

	/*t.previousTableName = tableName
	t.previousColumns = columns

	t.Unlock()*/
	return r
}

func (ch *CHInserter) Insert(rows []*Row) (result error) {
	tx, err := ch.DB.Begin()
	if err != nil {
		return errors.Wrap(err)
	}

	// rows may use different SQL "statements"
	stmtMap := map[string]*sql.Stmt{}
	for _, row := range rows {
		stmtString := getSQLStatementStringForRower.GetSQLStatementStringForRow(row)
		stmt := stmtMap[stmtString]
		if stmt == nil {
			stmt, err = tx.Prepare(stmtString)
			if err != nil {
				ch.LogWarning(errors.Wrap(err))
				result = err // TODO: fix this lame error reporting
				continue
			}
			defer stmt.Close()
			stmtMap[stmtString] = stmt
		}

		_, err := stmt.Exec(row.GetValues()...)
		if err != nil {
			ch.LogWarning(errors.Wrap(err))
			result = err // TODO: fix this lame error reporting
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err)
	}

	return
}
