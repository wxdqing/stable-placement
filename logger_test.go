package stableplacement

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestStdLoggerWritesLevels(t *testing.T) {
	var output bytes.Buffer
	logger := NewStdLogger(log.New(&output, "", 0))

	logger.Debugf("debug %d", 1)
	logger.Warnf("warn %d", 2)
	logger.Errorf("error %d", 3)

	text := output.String()
	for _, want := range []string{"[DEBUG] debug 1", "[WARN] warn 2", "[ERROR] error 3"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output %q does not contain %q", text, want)
		}
	}
}

func TestNopLogger(t *testing.T) {
	var logger Logger = NopLogger{}
	logger.Debugf("debug")
	logger.Warnf("warn")
	logger.Errorf("error")
}
