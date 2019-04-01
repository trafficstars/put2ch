package main

import (
	"flag"
	"log"

	"github.com/trafficstars/udp2ch"
)

func fatalIf(err error) {
	if err == nil {
		return
	}

	log.Fatal(err)
}

func main() {
	var port = flag.Int(`port`, 5363, `UDP port to be listened (default: 5363)`)
	var tableName = flag.String(`table-name`, `log`, `table to be inserted to (default: "log")`)
	var inputFormat = flag.String(`input-format`, `rawjson`, `input data format (possible values: rawjson; default: "rawjson")`)
	var dataColumnName = flag.String(`data-column-name`, `raw`, `if input format is "rawjson" then it's required to select a column to write to (default: "rawjson")`)
	var dateColumnName = flag.String(`date-column-name`, `received_at`, `if input format is "rawjson" then it's required to select a column to be used for log receive dates (default: "received_at")`)
	var chDSN = flag.String(`ch-dsn`, `tcp://127.0.0.1:9000`, `DSN for the ClickHouse connection (default: "tcp://127.0.0.1:9000")`)
	flag.Parse()

	var getRower udp2ch.GetRower
	switch *inputFormat {
	case `rawjson`:
		var err error
		getRower, err = udp2ch.NewRawJSONUDPListener(uint16(*port), *tableName, *dataColumnName, *dateColumnName, &logger{})
		fatalIf(err)
	default:
		log.Fatalf(`unknown input format: "%v"`, *inputFormat)
	}

	chInserter, err := udp2ch.NewCHInserter(*chDSN, getRower, &logger{})
	fatalIf(err)
	fatalIf(chInserter.Loop())
}
