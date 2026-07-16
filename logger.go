package stableplacement

import "log"

// Logger 表示 stable-placement 运行时所需的最小日志能力。
type Logger interface {
	Debugf(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// StdLogger 使用标准库 log.Logger 输出日志。
type StdLogger struct {
	Base *log.Logger
}

// NewStdLogger 创建标准库日志实现；base 为 nil 时使用 log.Default()。
func NewStdLogger(base *log.Logger) StdLogger {
	return StdLogger{Base: base}
}

// DefaultLogger 返回使用标准库默认 logger 的日志实现。
func DefaultLogger() Logger {
	return StdLogger{Base: log.Default()}
}

// Debugf 输出调试日志。
func (l StdLogger) Debugf(format string, args ...any) {
	l.logger().Printf("[DEBUG] "+format, args...)
}

// Warnf 输出警告日志。
func (l StdLogger) Warnf(format string, args ...any) {
	l.logger().Printf("[WARN] "+format, args...)
}

// Errorf 输出错误日志。
func (l StdLogger) Errorf(format string, args ...any) {
	l.logger().Printf("[ERROR] "+format, args...)
}

func (l StdLogger) logger() *log.Logger {
	if l.Base != nil {
		return l.Base
	}
	return log.Default()
}

// NopLogger 丢弃所有日志。
type NopLogger struct{}

// Debugf 丢弃调试日志。
func (NopLogger) Debugf(string, ...any) {}

// Warnf 丢弃警告日志。
func (NopLogger) Warnf(string, ...any) {}

// Errorf 丢弃错误日志。
func (NopLogger) Errorf(string, ...any) {}
