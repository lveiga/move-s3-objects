package errors

import (
	"fmt"
	"os"
)

// ExitErrorf ...
func ExitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// SpreadErrors ...
func SpreadErrors(args ...string) string {
	message := "errors: /n"
	for _, v := range args {
		message = message + " " + v
	}

	return message
}
