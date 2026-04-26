package logutil

import "runtime/debug"

func StackTrace() string {
	return string(debug.Stack())
}
