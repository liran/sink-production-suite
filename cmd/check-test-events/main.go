// Command check-test-events rejects incomplete or skipped qualification runs.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/liran/sink-production-suite/internal/testevents"
)

func main() {
	file := flag.String("file", "", "go test -json evidence file")
	require := flag.String("require", "", "comma-separated required test names")
	expectFailure := flag.String("expect-failure", "", "historical test that must fail")
	assertion := flag.String("assertion", "", "required historical assertion text")
	flag.Parse()
	opts := checkOptions{file: *file, require: *require, expectFailure: *expectFailure, assertion: *assertion}
	if err := check(opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type checkOptions struct {
	file          string
	require       string
	expectFailure string
	assertion     string
}

func check(opts checkOptions) error {
	file, err := os.Open(opts.file)
	if err != nil {
		return err
	}
	defer file.Close()
	if opts.expectFailure != "" {
		return testevents.CheckFailure(file, opts.expectFailure, opts.assertion)
	}
	var required []string
	if opts.require != "" {
		required = strings.Split(opts.require, ",")
	}
	return testevents.Check(file, required)
}
