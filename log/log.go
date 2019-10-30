package log

import (
	"fmt"
)

type Level int

const (
	LevelDebug Level = 0
	LevelInfo  Level = 1
	LevelWarn  Level = 2
	LevelError Level = 3
	LevelFatal Level = 4
)

var prefixes = []string{
	"[DEBUG]", "[INFO]", "[WARN]", "[ERROR]", "[FATAL]",
}

type Log struct {
	CurrentLevel  Level
	ProjectPrefix string
}

// Init the app trace log level
func (this *Log) Init(logLevel string, ProjectPrefix string) {
	// set log level
	fmt.Println("logLevel is :", logLevel)
	this.ProjectPrefix = ProjectPrefix
	switch logLevel {
	case "debug":
		this.CurrentLevel = LevelDebug
	case "info":
		this.CurrentLevel = LevelInfo
	case "warn":
		this.CurrentLevel = LevelWarn
	case "error":
		this.CurrentLevel = LevelError
	case "fatal":
		this.CurrentLevel = LevelFatal
	}
}

func (this *Log) Debug(str string, a ...interface{}) {
	_, err := this._print(LevelDebug, str, a...)
	if err != nil {
		println(err)
	}
	return
}

func (this *Log) Info(str string, a ...interface{}) {
	_, err := this._print(LevelInfo, str, a...)
	if err != nil {
		println(err)
	}
	return
}

func (this *Log) Warn(str string, a ...interface{}) {
	_, err := this._print(LevelWarn, str, a...)
	if err != nil {
		println(err)
	}
	return
}

func (this *Log) Error(str string, a ...interface{}) {
	_, err := this._print(LevelError, str, a...)
	if err != nil {
		println(err)
	}
	return
}

func (this *Log) Fatal(str string, a ...interface{}) {
	_, err := this._print(LevelFatal, str, a...)
	if err != nil {
		println(err)
	}
	return
}

func (this *Log) _print(level Level, str string, a ...interface{}) (n int, err error) {
	if level < this.CurrentLevel {
		return 0, nil
	}
	return fmt.Printf(this.ProjectPrefix+prefixes[level]+str+"\n", a...)
}
