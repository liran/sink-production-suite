package testevents

import (
	"strings"
	"testing"
)

func TestRejectsFalseGreenQualification(t *testing.T) {
	start := `{"Action":"start","Package":"suite"}` + "\n"
	run := `{"Action":"run","Package":"suite","Test":"TestContract"}` + "\n"
	pass := `{"Action":"pass","Package":"suite","Test":"TestContract"}` + "\n"
	finish := `{"Action":"pass","Package":"suite"}` + "\n"
	valid := start + run + pass + finish
	cases := []struct {
		name      string
		data      string
		required  []string
		wantError bool
	}{
		{name: "valid", data: valid, required: []string{"TestContract"}},
		{name: "empty", wantError: true},
		{name: "package only", data: start + finish, wantError: true},
		{name: "skip", data: start + run + `{"Action":"skip","Test":"TestContract"}` + finish, wantError: true},
		{name: "failure", data: valid + `{"Action":"fail","Package":"other"}`, wantError: true},
		{name: "unfinished package", data: start + run + pass, wantError: true},
		{name: "unfinished test", data: start + run + finish, wantError: true},
		{name: "missing contract", data: valid, required: []string{"TestIncident"}, wantError: true},
		{name: "truncated json", data: valid + `{`, wantError: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Check(strings.NewReader(tc.data), tc.required)
			if (err != nil) != tc.wantError {
				t.Fatalf("Check() = %v, wantError=%t", err, tc.wantError)
			}
		})
	}
}

func TestHistoricalFailureRequiresTheIncidentAssertion(t *testing.T) {
	ran := `{"Action":"run","Test":"TestIncident/backend"}`
	assertion := `{"Action":"output","OutputType":"error","Test":"TestIncident/backend","Output":"lost acknowledged write"}`
	failed := `{"Action":"fail","Test":"TestIncident/backend"}`
	for _, tc := range []struct {
		name      string
		data      string
		wantError bool
	}{
		{name: "incident", data: ran + assertion + failed},
		{name: "arbitrary failure", data: ran + failed, wantError: true},
		{name: "message without failure", data: ran + assertion, wantError: true},
		{name: "build failure", data: `{"Action":"build-fail"}`, wantError: true},
		{name: "skip", data: ran + `{"Action":"skip"}`, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckFailure(strings.NewReader(tc.data), "TestIncident/backend", "lost acknowledged write")
			if (err != nil) != tc.wantError {
				t.Fatalf("CheckFailure()=%v, wantError=%t", err, tc.wantError)
			}
		})
	}
}
