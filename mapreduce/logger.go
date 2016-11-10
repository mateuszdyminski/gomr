package mapreduce

import (
	"fmt"
	"log"
)

// Logger hold bacic info about logger configuration.
type Logger struct {
	prefix string
	debug  bool
}

// NewLogger returns new Logger instance.
func NewLogger(prefix string, debug bool) *Logger {
	return &Logger{prefix: prefix, debug: debug}
}

// Info logs the formatted message with level INFO.
func (l *Logger) Info(format string, v ...interface{}) {
	log.Printf("%s [INFO] %s\n", l.prefix, fmt.Sprintf(format, v...))
}

// Debug logs the formatted message with level DEBUG - only if debug flag is set to true.
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.debug {
		log.Printf("%s [DEBUG] %s\n", l.prefix, fmt.Sprintf(format, v...))
	}
}

// Error logs the formatted message with level ERROR.
func (l *Logger) Error(format string, v ...interface{}) {
	log.Printf("%s [ERROR] %s\n", l.prefix, fmt.Sprintf(format, v...))
}

// Fatal logs the formatted message with level FATAL.
func (l *Logger) Fatal(format string, v ...interface{}) {
	log.Fatalf("%s [FATAL] %s\n", l.prefix, fmt.Sprintf(format, v...))
}
