package put2ch

type Logger interface {
	Error(...interface{})
	Warning(...interface{})
	Trace(...interface{})
}

type dummyLoggerT struct {

}

var dummyLogger Logger = &dummyLoggerT{}

func (l *dummyLoggerT) Error(...interface{}) {}
func (l *dummyLoggerT) Warning(...interface{}) {}
func (l *dummyLoggerT) Trace(...interface{}) {}