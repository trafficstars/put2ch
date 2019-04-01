package udp2ch

import (
	"bytes"
	"encoding/json"
	"net"
	"time"

	"github.com/pkg/errors"
)

const (
	rowQueueLength = 128
)

type XUDPListener struct {
	net.Conn

	Logger Logger
}

func (l *XUDPListener) LogError(args ...interface{}) {
	if l.Logger == nil {
		return
	}
	l.Logger.Error(args...)
}

func (l *XUDPListener) LogWarning(args ...interface{}) {
	if l.Logger == nil {
		return
	}
	l.Logger.Warning(args...)
}

type RawJSONUDPListener struct {
	XUDPListener

	*json.Decoder
	*json.Encoder

	encoderResult bytes.Buffer

	TableName string
	Columns   []string

	rowC chan *Row

	isRunning bool
}

func NewRawJSONUDPListener(port uint16, tableName, dataColumnName, dateColumnName string, logger Logger) (*RawJSONUDPListener, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: int(port)})
	if err != nil {
		return nil, err
	}

	l := &RawJSONUDPListener{
		TableName: tableName,
		Columns:   []string{dateColumnName, dataColumnName},
	}
	l.Logger = logger
	l.Conn = conn

	l.Decoder = json.NewDecoder(l)
	l.Decoder.UseNumber()

	l.Encoder = json.NewEncoder(&l.encoderResult)

	// This line is not required (anyway this behaviour is the default one), but just in case...
	// ... CH requires no whitespaces outside of strings in JSONs,
	// see: https://clickhouse.yandex/docs/en/query_language/functions/json_functions/
	l.Encoder.SetIndent("", "")

	l.rowC = make(chan *Row, rowQueueLength)

	l.start()

	return l, nil
}

func (l *RawJSONUDPListener) GetRow() *Row {
	return <-l.rowC
}

func (l *RawJSONUDPListener) start() {
	go l.loop()
}

func (l *RawJSONUDPListener) loop() {
	l.isRunning = true
	for l.isRunning {
		msg := map[string]interface{}{}
		err := l.Decode(&msg)
		if err != nil {
			if !l.isRunning {
				break
			}
			buf := newBuffer()
			buf.ReadFrom(l.Decoder.Buffered())
			l.LogWarning(errors.Wrap(err, `(*RawJSONUDPListener).loop(): unable to decode`), buf.String())
			buf.Release()

			// TODO: remove this dirty hack. It's required to find a way to just reset the decoder
			// (instead of re-creating it)
			l.Decoder = json.NewDecoder(l)
			l.Decoder.UseNumber()
			continue
		}

		err = l.Encode(msg)
		if err != nil {
			l.LogWarning(errors.Wrap(err, `(*RawJSONUDPListener).loop(): unable to encode`))
		}

		row := NewRow()
		row.tableName = l.TableName
		row.columns = l.Columns
		row.values = []interface{}{time.Now(), l.encoderResult.String()}
		l.rowC <- row

		l.encoderResult.Reset()
	}
}

func (l *RawJSONUDPListener) Close() {
	l.isRunning = false
	l.Conn.Close()
}
