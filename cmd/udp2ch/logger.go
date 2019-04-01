package main

import (
	"log"
)

type logger struct{
	warnCounter int
}

func (l *logger) Error(args ...interface{}) {
	log.Print(`[error] `, args)
}

func (l *logger) Warning(args ...interface{}) {
	if l.warnCounter % 1000 == 0 {
		log.Print(`[warning] `, args)
	}
	l.warnCounter++
}
