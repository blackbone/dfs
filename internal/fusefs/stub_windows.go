//go:build windows

package fusefs

import (
	"context"
	"errors"
	"time"
)

const errMsg = "fusefs unsupported on windows"

var errUnsupported = errors.New(errMsg)

func Mount(string, string) error { return errUnsupported }

func Watch(context.Context, string) error { return errUnsupported }

func Check(context.Context, string, time.Duration) {}
