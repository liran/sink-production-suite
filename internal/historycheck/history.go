// Package historycheck checks completed single-record histories against a
// sequential integer-document specification, independently of Sink and Lua.
package historycheck

import (
	"errors"
	"fmt"
)

type Kind string

const (
	Read    Kind = "read"
	Set     Kind = "set"
	Create  Kind = "create"
	Replace Kind = "replace"
	Add     Kind = "add"
	Delete  Kind = "delete"
)

type State struct {
	Found   bool
	Counter int
}

type Result struct {
	Applied  bool
	Conflict bool  // A definite CAS exhaustion, with no committed effect.
	State    State // Populated only for reads.
}

type Entry struct {
	ID     int
	Client int
	Start  int64
	End    int64
	Kind   Kind
	Input  int
	Result Result
}

type searchState struct {
	visited uint64
	value   State
}

// Check searches for an ordering that explains every result and preserves
// real-time precedence. It fails closed on malformed input or search exhaustion;
// uncertain/pending RPCs must not be silently removed from a history.
func Check(initial State, history []Entry) error {
	if len(history) == 0 || len(history) > 24 {
		return errors.New("history must contain between 1 and 24 completed calls")
	}
	if !initial.Found && initial.Counter != 0 {
		return errors.New("absent initial state contains a counter")
	}
	ids := make(map[int]bool)
	predecessors := make([]uint64, len(history))
	for i, entry := range history {
		if ids[entry.ID] || entry.Start <= 0 || entry.End <= entry.Start {
			return fmt.Errorf("invalid identity or interval for entry %d", i)
		}
		ids[entry.ID] = true
		switch entry.Kind {
		case Read, Set, Create, Replace, Add, Delete:
		default:
			return fmt.Errorf("unknown operation %q", entry.Kind)
		}
		for j, before := range history {
			if before.End < entry.Start {
				predecessors[i] |= uint64(1) << j
			}
		}
	}
	complete := uint64(1)<<len(history) - 1
	rejected := make(map[searchState]bool)
	exhausted := false
	var visit func(searchState) bool
	visit = func(current searchState) bool {
		if current.visited == complete {
			return true
		}
		if rejected[current] || exhausted {
			return false
		}
		if len(rejected) >= 250000 {
			exhausted = true
			return false
		}
		for i, entry := range history {
			bit := uint64(1) << i
			if current.visited&bit != 0 || predecessors[i]&current.visited != predecessors[i] {
				continue
			}
			value, result := apply(current.value, entry)
			if result != entry.Result {
				continue
			}
			next := searchState{visited: current.visited | bit, value: value}
			if visit(next) {
				return true
			}
		}
		rejected[current] = true
		return false
	}
	start := searchState{value: initial}
	if visit(start) {
		return nil
	}
	if exhausted {
		return errors.New("history search budget exhausted; no correctness conclusion")
	}
	return errors.New("no legal sequential ordering respects all results and real-time precedence")
}

func apply(state State, entry Entry) (State, Result) {
	result := Result{Applied: true}
	if entry.Result.Conflict && (entry.Kind == Add || entry.Kind == Replace) {
		result.Applied, result.Conflict = false, true
		return state, result
	}
	switch entry.Kind {
	case Read:
		result.Applied = false
		result.State = state
	case Delete:
		state = State{}
	case Create:
		result.Applied = !state.Found
		if result.Applied {
			state.Found, state.Counter = true, entry.Input
		}
	case Replace:
		result.Applied = state.Found
		if result.Applied {
			state.Counter = entry.Input
		}
	case Set:
		state.Found, state.Counter = true, entry.Input
	case Add:
		state.Found, state.Counter = true, state.Counter+entry.Input
	}
	return state, result
}
