// Package testevents validates qualification evidence from go test -json.
package testevents

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type event struct {
	Action     string
	Package    string
	Test       string
	Output     string
	OutputType string
}

// CheckFailure proves that a historical candidate reached a specific assertion.
// A build failure, missing infrastructure, skip or arbitrary nonzero exit is
// never sufficient evidence that a regression test detects its incident.
func CheckFailure(reader io.Reader, test, assertion string) error {
	if test == "" || assertion == "" {
		return errors.New("historical check requires a test and assertion")
	}
	decoder := json.NewDecoder(reader)
	ran, failed, matched := false, false, false
	for {
		var entry event
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if entry.Action == "skip" || entry.Action == "build-fail" {
			return fmt.Errorf("historical check contains %s", entry.Action)
		}
		if entry.Test != test {
			continue
		}
		switch entry.Action {
		case "run":
			ran = true
		case "fail":
			failed = true
		case "output":
			if entry.OutputType == "error" && strings.Contains(entry.Output, assertion) {
				matched = true
			}
		}
	}
	if !ran || !failed || !matched {
		return fmt.Errorf("historical assertion not demonstrated: test=%s ran=%t failed=%t matched=%t", test, ran, failed, matched)
	}
	return nil
}

// Check rejects skipped or incomplete tests and empty/package-only runs. Each
// named requirement must have an explicit test pass event in this invocation.
func Check(reader io.Reader, required []string) error {
	decoder := json.NewDecoder(reader)
	started := make(map[string]bool)
	passed := make(map[string]bool)
	packages := make(map[string]bool)
	tests := 0
	for {
		var entry event
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("decode test event: %w", err)
		}
		key := entry.Package + "/" + entry.Test
		switch entry.Action {
		case "start":
			packages[entry.Package] = false
		case "run":
			started[key] = false
		case "skip", "fail", "build-fail":
			return fmt.Errorf("qualification contains %s: %s", entry.Action, key)
		case "pass":
			if entry.Test == "" {
				packages[entry.Package] = true
			} else {
				if _, ok := started[key]; !ok {
					return fmt.Errorf("pass without run: %s", key)
				}
				started[key] = true
				passed[entry.Test] = true
				tests++
			}
		}
	}
	if tests == 0 || len(packages) == 0 {
		return errors.New("qualification ran no tests or has no package result")
	}
	for name, complete := range packages {
		if !complete {
			return fmt.Errorf("package has no terminal pass: %s", name)
		}
	}
	for name, complete := range started {
		if !complete {
			return fmt.Errorf("test has no terminal pass: %s", name)
		}
	}
	for _, name := range required {
		if !passed[name] {
			return fmt.Errorf("required test did not pass: %s", name)
		}
	}
	return nil
}
