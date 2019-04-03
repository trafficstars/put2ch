package put2ch

import (
	"database/sql"
	"sort"
	"time"

	_ "github.com/kshvakov/clickhouse"
	"github.com/xaionaro-go/errors"
	"github.com/xaionaro-go/spinlock"
)

const (
	defaultBunchSize = 10000
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

	tableStructureByName map[string]*tableStructure
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

	if logger == nil {
		logger = dummyLogger
	}

	return &CHInserter{
		DB:        db,
		RowsChan:  rowsChan,
		BunchSize: defaultBunchSize,
		Logger:    logger,

		tableStructureByName: map[string]*tableStructure{},
	}, nil
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

		err = ch.insert(ch.Queue)
		if err == nil {
			ch.Queue.Release()
		}
	})

	return
}

func (ch *CHInserter) getStatementString(row *Row) string {
	tableName := row.GetTableName()
	tableStructure := ch.getTableStructure(tableName)
	columns := tableStructure.Columns

	buf := newBuffer()
	buf.WriteString("INSERT INTO `")
	buf.WriteString(tableName)
	buf.WriteString("` ( ")

	if len(columns) == 0 {
		panic(`len(columns) == 0`)
	}
	buf.WriteString(columns[0].Name)
	for _, column := range columns[1:] {
		buf.WriteString(`, `)
		buf.WriteString(column.Name)
	}
	buf.WriteString(` ) VALUES ( ?`)
	for range columns[:1] {
		buf.WriteString(`,?`)
	}
	buf.WriteString(` )`)
	r := buf.String()
	buf.Release()

	return r
}

type columnValueType int

const (
	columnValueType_unknown = columnValueType(iota)
	columnValueType_string
	columnValueType_int64
	columnValueType_float64
	columnValueType_dateTime
	columnValueType_date
)

type tableColumn struct {
	Name string
	ValueType columnValueType
}
type tableColumns []*tableColumn

func (s tableColumns) Sort() {
	sort.Slice(s, func(i, j int) bool {
		return s[i].Name < s[j].Name
	})
}

func (s tableColumns) SearchByName(columnName string) *tableColumn {
	idx := sort.Search(len(s), func(idx int) bool {
		return s[idx].Name >= columnName
	})
	if idx == -1 || idx >= len(s) {
		return nil
	}
	if s[idx].Name != columnName {
		return nil
	}
	return s[idx]
}

type tableStructure struct {
	Columns tableColumns
}

func (ch *CHInserter) getTableStructure(tableName string) *tableStructure {
	r := ch.tableStructureByName[tableName]
	if r != nil {
		return r
	}

	r = &tableStructure{}
	rows, err := ch.DB.Query("DESCRIBE TABLE " + tableName)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var columnValueTypeName, defaultType, extraArg1, extraArg2, extraArg3 string
		column := &tableColumn{}
		err = rows.Scan(&column.Name, &columnValueTypeName, &defaultType, &extraArg1, &extraArg2, &extraArg3)
		if err != nil {
			panic(err)
		}
		if defaultType != `` {
			continue
		}
		switch columnValueTypeName {
		case "DateTime", "Nullable(DateTime)":
			column.ValueType = columnValueType_dateTime
		case "Date", "Nullable(Date)":
			column.ValueType = columnValueType_date
		case "UInt64", "Int64", "Nullable(UInt64)", "Nullable(Int64)":
			column.ValueType = columnValueType_int64
		case "String", "Nullable(String)":
			column.ValueType = columnValueType_string
		case "Float64", "Nullable(Float64)":
			column.ValueType = columnValueType_float64
		}
		r.Columns = append(r.Columns, column)
	}

	r.Columns.Sort()

	ch.tableStructureByName[tableName] = r
	return r
}

func (ch *CHInserter) fixTableStructureForRow (row *Row) {
	tableStructure := ch.getTableStructure(row.GetTableName())

	var columnsToAdd tableColumns

	for idx, columnName := range row.columns {
		valueType := columnValueType_unknown
		switch row.values[idx].(type) {
		case int64, uint64:
			valueType = columnValueType_int64
		case float64:
			valueType = columnValueType_float64
		case string:
			valueType = columnValueType_string
		case time.Time:
			valueType = columnValueType_dateTime
		}
		column := tableStructure.Columns.SearchByName(columnName)
		if column == nil {
			column = &tableColumn{
				Name: columnName,
				ValueType: valueType,
			}
			columnsToAdd = append(columnsToAdd, column)
			continue
		}

		if column.ValueType != valueType {
			row.values[idx] = nil
			continue
		}
	}

	for _, column := range columnsToAdd {
		var chValueTypeName string
		switch column.ValueType {
		case columnValueType_string:
			chValueTypeName = "String"
		case columnValueType_float64:
			chValueTypeName = "Float64"
		case columnValueType_int64:
			chValueTypeName = "Int64"
		default:
			continue
		}
		query := "ALTER TABLE "+row.GetTableName()+" ADD COLUMN `" + column.Name + "` Nullable(" + chValueTypeName+")"
		ch.Logger.Info(`adding column:`, query)
		_, err := ch.DB.Exec(query)
		if err != nil {
			panic(err)
		}
	}

	if len(columnsToAdd) != 0 {
		//ch.Lock()
		ch.tableStructureByName[row.GetTableName()] = nil
		//ch.Unlock()
	}
}

func (ch *CHInserter) insert(rows []*Row) (result error) {
	if len(rows) < 1 {
		return
	}
	rowSample := rows[0]

	// rows may use different SQL "statements"
	for _, row := range rows {
		ch.fixTableStructureForRow(row)
		if row.GetTableName() != rowSample.GetTableName() {
			return errors.NotImplemented.New("multiple tables are not supported, yet")
		}
	}

	tx, err := ch.DB.Begin()
	if err != nil {
		return errors.Wrap(err)
	}

	stmtString := ch.getStatementString(rowSample)
	stmt, err := tx.Prepare(stmtString)
	if err != nil {
		tx.Rollback()

		err = errors.Wrap(err, stmtString)
		ch.Logger.Warning(err)
		return errors.Wrap(err)
	}
	defer stmt.Close()

	ch.Logger.Trace("statement", stmtString)

	for _, row := range rows {
		values := row.GetValuesForTable(ch.getTableStructure(row.GetTableName()))
		ch.Logger.Trace("values", values)
		_, err := stmt.Exec(values...)
		if err != nil {
			ch.Logger.Warning(errors.Wrap(err))
			result = err // TODO: fix this lame error reporting
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err)
	}

	return
}
