package logger

import (
	"io"
	"log/slog"
)

func New(logLevel slog.Leveler, logOutput io.Writer) *slog.Logger {
	opts := slog.HandlerOptions{
		Level: logLevel,
	}

	// if logLevel == slog.LevelDebug {
	// 	opts.AddSource = true
	// }

	jsonHandler := slog.NewJSONHandler(logOutput, &opts)

	return slog.New(jsonHandler)
}
