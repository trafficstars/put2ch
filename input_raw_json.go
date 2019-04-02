package put2ch

import (
	"bytes"
	"encoding/json"
	"github.com/xaionaro-go/errors"
	"io"
	"net"
	"time"
)

type InputRawJSON struct {
	*json.Decoder
	*json.Encoder

	Logger Logger
	Reader io.ReadCloser
	
	encoderResult bytes.Buffer
	
	TableName string
	Columns   []string

	OutChan chan *Row

	isRunning bool
}

func NewInputRawJSON(reader io.ReadCloser, OutChan chan *Row, tableName, dataColumnName, dateColumnName string, logger Logger) *InputRawJSON {
	input := &InputRawJSON{}
	
	if logger == nil {
		logger = dummyLogger
	}
	input.Logger = logger

	input.OutChan = OutChan

	input.Reader = reader 
	input.TableName = tableName
	input.Columns   = []string{dateColumnName, dataColumnName}

	input.Decoder = json.NewDecoder(input.Reader)
	input.Decoder.UseNumber()

	input.Encoder = json.NewEncoder(&input.encoderResult)

	// This line is not required (anyway this behaviour is the default one), but just in case...
	// ... CH requires no whitespaces outside of strings in JSONs,
	// see: https://clickhouse.yandex/docs/en/query_language/functions/json_functions/
	input.Encoder.SetIndent("", "")

	input.start()

	return input
}

func (l *InputRawJSON) start() {
	go l.loop()
}

func (l *InputRawJSON) loop() {
	l.isRunning = true
	for l.isRunning {
		msg := map[string]interface{}{}
		l.Logger.Trace(`S`)
		err := l.Decode(&msg)
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
			buf.ReadFrom(l.Decoder.Buffered())
			l.Logger.Warning(errors.Wrap(err, `(*RawJSONUDPListener).loop(): unable to decode`), buf.String())
			buf.Release()

			// TODO: remove this dirty hack. It's required to find a way to just reset the decoder
			// (instead of re-creating it)
			l.Decoder = json.NewDecoder(l.Reader)
			l.Decoder.UseNumber()
			continue
		}

		err = l.Encode(msg)
		if err != nil {
			l.Logger.Warning(errors.Wrap(err, `(*RawJSONUDPListener).loop(): unable to encode`))
		}

		row := NewRow()
		row.tableName = l.TableName
		row.columns = l.Columns
		row.values = []interface{}{time.Now(), l.encoderResult.String()}
		l.Logger.Trace(`Q`)
		l.OutChan <- row
		l.Logger.Trace(`/Q`)

		l.encoderResult.Reset()
	}
}

func (l *InputRawJSON) Close() error {
	l.isRunning = false
	return l.Close()
}
