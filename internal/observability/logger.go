package observability

import (
	"os"

	"github.com/rs/zerolog"
)

// Logger is the global structured logger.
var Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()

// Common field names for structured logs.
const (
	FieldAddress = "addr"
	FieldPath    = "path"
	FieldError   = "err"
)
