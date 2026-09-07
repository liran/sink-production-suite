package historycheck

import "testing"

func TestCheckerRejectsImpossibleHistories(t *testing.T) {
	empty := State{}
	foundOne := State{Found: true, Counter: 1}
	foundTwo := State{Found: true, Counter: 2}
	applied := Result{Applied: true}
	readOne := Result{State: foundOne}
	readTwo := Result{State: foundTwo}
	readAbsent := Result{}
	conflict := Result{Conflict: true}
	cases := []struct {
		name    string
		history []Entry
	}{
		{name: "lost increment", history: []Entry{
			{ID: 0, Start: 1, End: 4, Kind: Add, Input: 1, Result: applied},
			{ID: 1, Start: 2, End: 3, Kind: Add, Input: 1, Result: applied},
			{ID: 2, Start: 5, End: 6, Kind: Read, Result: readOne},
		}},
		{name: "duplicate application", history: []Entry{
			{ID: 0, Start: 1, End: 2, Kind: Add, Input: 1, Result: applied},
			{ID: 1, Start: 3, End: 4, Kind: Read, Result: readTwo},
		}},
		{name: "two successful creates", history: []Entry{
			{ID: 0, Start: 1, End: 4, Kind: Create, Input: 1, Result: applied},
			{ID: 1, Start: 2, End: 3, Kind: Create, Input: 2, Result: applied},
		}},
		{name: "real-time stale read", history: []Entry{
			{ID: 0, Start: 1, End: 2, Kind: Set, Input: 1, Result: applied},
			{ID: 1, Start: 3, End: 4, Kind: Read, Result: readAbsent},
		}},
		{name: "failed replace changed state", history: []Entry{
			{ID: 0, Start: 1, End: 2, Kind: Replace, Input: 1, Result: readAbsent},
			{ID: 1, Start: 3, End: 4, Kind: Read, Result: readOne},
		}},
		{name: "resurrection after delete", history: []Entry{
			{ID: 0, Start: 1, End: 2, Kind: Set, Input: 1, Result: applied},
			{ID: 1, Start: 3, End: 4, Kind: Delete, Result: applied},
			{ID: 2, Start: 5, End: 6, Kind: Read, Result: readOne},
		}},
		{name: "conflict incorrectly applied", history: []Entry{
			{ID: 0, Start: 1, End: 2, Kind: Add, Input: 1, Result: conflict},
			{ID: 1, Start: 3, End: 4, Kind: Read, Result: readOne},
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := Check(empty, test.history); err == nil {
				t.Fatal("checker accepted a deliberately corrupted history")
			}
		})
	}
}

func TestCheckerBacktracksAndPreservesOverlappingReads(t *testing.T) {
	empty := State{}
	applied := Result{Applied: true}
	foundOne := State{Found: true, Counter: 1}
	readOne := Result{State: foundOne}
	// The array order is deliberately incompatible with the only valid order:
	// create(1), read(1), set(2). Arrival order must not decide the result.
	history := []Entry{
		{ID: 0, Start: 1, End: 6, Kind: Set, Input: 2, Result: applied},
		{ID: 1, Start: 2, End: 5, Kind: Create, Input: 1, Result: applied},
		{ID: 2, Start: 3, End: 4, Kind: Read, Result: readOne},
	}
	if err := Check(empty, history); err != nil {
		t.Fatal(err)
	}
	// An overlapping read may legally see the state before a write.
	history = []Entry{
		{ID: 0, Start: 1, End: 4, Kind: Set, Input: 1, Result: applied},
		{ID: 1, Start: 2, End: 3, Kind: Read},
	}
	if err := Check(empty, history); err != nil {
		t.Fatal(err)
	}
}

func TestCheckerRejectsIncompleteOrEmptyEvidence(t *testing.T) {
	empty := State{}
	if err := Check(empty, nil); err == nil {
		t.Fatal("empty history passed")
	}
	history := []Entry{{ID: 0, Start: 1, Kind: Read}}
	if err := Check(empty, history); err == nil {
		t.Fatal("incomplete history passed")
	}
}
