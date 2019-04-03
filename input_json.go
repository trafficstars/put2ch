package put2ch

import (
	// json "github.com/francoispqt/gojay"
	"encoding/json"
	"io"
	"net"
	"strings"
	"time"

	"github.com/xaionaro-go/errors"
)

type InputJSON struct {
	Logger Logger
	Reader io.ReadCloser

	TableName string

	OutChan chan *Row
	DateColumnName string

	TryParseJSONInFields []string

	isRunning bool
}

func NewInputJSON(reader io.ReadCloser, OutChan chan *Row, tableName, dateColumnName string, tryParseJSONInFields []string, logger Logger) *InputJSON {
	input := &InputJSON{}

	if logger == nil {
		logger = dummyLogger
	}
	input.Logger = logger

	input.OutChan = OutChan

	input.Reader = reader
	input.TableName = tableName
	input.DateColumnName = dateColumnName

	for _, fieldName := range tryParseJSONInFields {
		if fieldName == `` {
			continue
		}
		input.TryParseJSONInFields = append(input.TryParseJSONInFields, fieldName)
	}

	input.start()

	return input
}

func (l *InputJSON) start() {
	go l.loop()
}

func (l *InputJSON) addMsgToRow(row *Row, msg map[string]interface{}, prefix string) {
	for k, vI := range msg {
		if len(k) >= 64 {
			continue
		}
		postfix := ``
		switch v := vI.(type) {
		case json.Number:
			if i, err := v.Int64(); err == nil {
				row.values = append(row.values, i)
				postfix = `.int`
			} else
			if f, err := v.Float64(); err == nil {
				row.values = append(row.values, f)
				postfix = `.float`
			} else {
				row.values = append(row.values, v.String())
				postfix = `.string`
			}
		case string:
			if prefix == `` {
				shouldTryParseJSON := false
				for _, fieldName := range l.TryParseJSONInFields {
					if k == fieldName {
						shouldTryParseJSON = true
						break
					}
				}
				if shouldTryParseJSON {
					subMsg := map[string]interface{}{}
					if json.Unmarshal([]byte(v), &subMsg) == nil {
						l.addMsgToRow(row, subMsg,prefix+k+`.`)
						continue
					}
				}
			}
			row.values = append(row.values, v)
			postfix = `.string`
		case map[string]interface{}:
			l.addMsgToRow(row, v, prefix+k+`.`)
			continue
		default:
			continue
		}
		columnName := strings.Map(func (r rune) rune {
			switch {
			case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
				return r
			default:
				return '_'
			}
		}, k)
		columnName = strings.Trim(columnName, `.`)
		if len(columnName) == 0 {
			columnName = `zero_length_column`
		}
		if columnName[0] >= '0' && columnName[0] <= '9' {
			columnName = "f" + columnName
		}
		row.columns = append(row.columns, prefix+columnName+postfix)
	}
}

func (l *InputJSON) loop() {
	decoder := json.NewDecoder(l.Reader)
	decoder.UseNumber()

	l.isRunning = true
	for l.isRunning {
		msg := map[string]interface{}{}

		l.Logger.Trace(`S`)
		err := decoder.Decode(&msg)
		l.Logger.Trace(`/S`)
		if err != nil {
			if !l.isRunning {
				break
			}
			if err == io.EOF {
				// Closed by other side
				l.Close()
				continue
			}
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				// Timeout
				l.Close()
				continue
			}
			buf := newBuffer()
			buf.ReadFrom(decoder.Buffered()) // TODO: implement the method "Buffered()" within "gojay"
			l.Logger.Warning(errors.Wrap(err, `(*InputRawJSON).loop(): unable to decode`), buf.String())
			buf.Release()

			// TODO: remove this dirty hack. It's required to find a way to just reset the decoder
			// (instead of re-creating it)
			decoder = json.NewDecoder(l.Reader)
			continue
		}

		row := NewRow()
		row.tableName = l.TableName
		row.columns = []string{l.DateColumnName}
		row.values = []interface{}{time.Now()}
		l.addMsgToRow(row, msg, ``)
		l.Logger.Trace(`Q`)
		l.OutChan <- row
		l.Logger.Trace(`/Q`)
	}
}

func (l *InputJSON) Close() error {
	l.isRunning = false
	return l.Close()
}