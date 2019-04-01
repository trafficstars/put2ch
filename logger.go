package udp2ch

type Logger interface {
	Error(...interface{})
	Warning(...interface{})
}
