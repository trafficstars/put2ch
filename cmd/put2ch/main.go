package main

import (
	"flag"
	"fmt"
	"strings"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/trafficstars/put2ch"
)

func fatalIf(err error) {
	if err == nil {
		return
	}

	log.Fatal(err)
}

func newInput(reader io.ReadCloser, rowsChannel chan *put2ch.Row, inputFormat, tableName, dataColumnName, dateColumnName, tryParseJSONInFields string, logger put2ch.Logger) io.Closer {
	switch inputFormat {
	case `json`:
		return put2ch.NewInputJSON(reader, rowsChannel, tableName, dateColumnName, strings.Split(tryParseJSONInFields, ","), logger)
	case `rawjson`:
		return put2ch.NewInputRawJSON(reader, rowsChannel, tableName, dataColumnName, dateColumnName, logger)
	default:
		log.Fatalf(`unknown input format: "%v"`, inputFormat)
	}
	return nil
}

func main() {
	var udpPort = flag.Int(`udp-port`, 5363, `UDP port to be listened (to disable: -1; default: 5363)`)
	var tcpPort = flag.Int(`tcp-port`, 5363, `TCP port to be listened (to disable: -1; default: 5363)`)
	var netPprofPort = flag.Int(`net-pprof-port`, 5364, `port to be used for "net/pprof" (to disable: -1; default: 5364)`)
	var tableName = flag.String(`table-name`, `log`, `table to be inserted to (default: "log")`)
	var inputFormat = flag.String(`input-format`, `json`, `input data format (possible values: json, rawjson; default: "json")`)
	var tryParseJSONInFields = flag.String(`try-parse-json-in-fields`, `message`, `only for input data format "json"; comma separated values (default value: "message"; to disable: "")`)
	var dataColumnName = flag.String(`data-column-name`, `raw`, `if input format is "rawjson" then it's required to select a column to write to (default: "rawjson")`)
	var dateColumnName = flag.String(`date-column-name`, `received_at`, `if input format is "rawjson" then it's required to select a column to be used for log receive dates (default: "received_at")`)
	var chDSN = flag.String(`ch-dsn`, `tcp://127.0.0.1:9000`, `DSN for the ClickHouse connection (default: "tcp://127.0.0.1:9000")`)
	flag.Parse()

	rowsChannel := make(chan *put2ch.Row, 65536)

	if *udpPort >= 0 {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: *udpPort})
		fatalIf(err)

		newInput(conn, rowsChannel, *inputFormat, *tableName, *dataColumnName, *dateColumnName, *tryParseJSONInFields, &logger{})
	}

	if *tcpPort >= 0 {
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{Port: *tcpPort})
		fatalIf(err)

		go func() {
			for {

				conn, err := listener.Accept()
				fatalIf(err)
				newInput(conn, rowsChannel, *inputFormat, *tableName, *dataColumnName, *dateColumnName, *tryParseJSONInFields, &logger{})
			}
		}()
	}

	if *netPprofPort >= 0 {
		go func() {
			fatalIf(http.ListenAndServe(fmt.Sprintf(`:%v`, *netPprofPort), nil))
		}()
	}

	chInserter, err := put2ch.NewCHInserter(*chDSN, rowsChannel, &logger{})
	fatalIf(err)
	fatalIf(chInserter.Loop())
}
