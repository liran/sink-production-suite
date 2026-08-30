package contract_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/liran/sink-production-suite/internal/fixture"
	"github.com/liran/sink-production-suite/internal/luatest"
	"github.com/liran/sink-production-suite/internal/reference"
	"github.com/liran/sink-production-suite/programs"
)

type productCase struct {
	name     string
	current  *reference.Product
	incoming *reference.Product
}

func TestProductMergeProgramMatchesReferenceModel(t *testing.T) {
	fullCurrent, fullIncoming := fixture.ProductPair()
	historyCurrent, historyIncoming := fixture.ProductHistoryPair()
	_, createIncoming := fixture.ProductPair()
	cases := []productCase{
		{name: "full update and eviction recovery", current: fullCurrent, incoming: fullIncoming},
		{name: "history deduplication and limits", current: historyCurrent, incoming: historyIncoming},
		{name: "missing document creation", incoming: createIncoming},
	}
	engine, err := luatest.New(programs.ProductMerge)
	if err != nil {
		t.Fatalf("luatest.New(product) error = %v", err)
	}
	defer engine.Close()
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			expected := reference.MergeProduct(testCase.current, testCase.incoming, fixture.BaseTime)
			actualJSON, err := engine.Merge(testCase.current, testCase.incoming, fixture.BaseTime)
			if err != nil {
				t.Fatalf("Merge() error = %v", err)
			}
			actual := &reference.Product{}
			if err := json.Unmarshal(actualJSON, actual); err != nil {
				t.Fatalf("decode product result: %v\n%s", err, actualJSON)
			}
			assertEqualJSON(t, expected, actual)
		})
	}
}

type offerCase struct {
	name     string
	current  *reference.Offer
	incoming *reference.Offer
}

func TestOfferMergeProgramMatchesReferenceModel(t *testing.T) {
	fullCurrent, fullIncoming := fixture.OfferPair()
	addressCurrent, addressIncoming := fixture.OfferAddressHistoryPair()
	_, createIncoming := fixture.OfferPair()
	cases := []offerCase{
		{name: "full update tracking IDs and eviction recovery", current: fullCurrent, incoming: fullIncoming},
		{name: "address deduplication and limit", current: addressCurrent, incoming: addressIncoming},
		{name: "missing document creation", incoming: createIncoming},
	}
	engine, err := luatest.New(programs.OfferMerge)
	if err != nil {
		t.Fatalf("luatest.New(offer) error = %v", err)
	}
	defer engine.Close()
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			expected := reference.MergeOffer(testCase.current, testCase.incoming, fixture.BaseTime)
			actualJSON, err := engine.Merge(testCase.current, testCase.incoming, fixture.BaseTime)
			if err != nil {
				t.Fatalf("Merge() error = %v", err)
			}
			actual := &reference.Offer{}
			if err := json.Unmarshal(actualJSON, actual); err != nil {
				t.Fatalf("decode offer result: %v\n%s", err, actualJSON)
			}
			assertEqualJSON(t, expected, actual)
		})
	}
}

func TestProductMergePreservesLargeIntegerPrecision(t *testing.T) {
	current, incoming := fixture.ProductPair()
	current.CommentCount = 9_007_199_254_740_993
	incoming.CommentCount = 0
	engine, err := luatest.New(programs.ProductMerge)
	if err != nil {
		t.Fatalf("luatest.New(product) error = %v", err)
	}
	defer engine.Close()
	actualJSON, err := engine.Merge(current, incoming, fixture.BaseTime)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	actual := &reference.Product{}
	if err := json.Unmarshal(actualJSON, actual); err != nil {
		t.Fatalf("decode product result: %v", err)
	}
	if actual.CommentCount != current.CommentCount {
		t.Fatalf("comment_count = %d, want %d", actual.CommentCount, current.CommentCount)
	}
}

func TestProductMergePreservesEmptyUIDArray(t *testing.T) {
	incoming := &reference.Product{}
	engine, err := luatest.New(programs.ProductMerge)
	if err != nil {
		t.Fatalf("luatest.New(product) error = %v", err)
	}
	defer engine.Close()
	expected := reference.MergeProduct(nil, incoming, fixture.BaseTime)
	actualJSON, err := engine.Merge(nil, incoming, fixture.BaseTime)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	actual := &reference.Product{}
	if err := json.Unmarshal(actualJSON, actual); err != nil {
		t.Fatalf("decode product result: %v", err)
	}
	if actual.UIDs == nil {
		t.Fatal("uids decoded as nil, want an empty array")
	}
	assertEqualJSON(t, expected, actual)
}

func TestProductMergeUppercasesUnicodeBrand(t *testing.T) {
	current, incoming := fixture.ProductPair()
	incoming.Brand = "café Straße 品牌"
	engine, err := luatest.New(programs.ProductMerge)
	if err != nil {
		t.Fatalf("luatest.New(product) error = %v", err)
	}
	defer engine.Close()
	expected := reference.MergeProduct(current, incoming, fixture.BaseTime)
	actualJSON, err := engine.Merge(current, incoming, fixture.BaseTime)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	actual := &reference.Product{}
	if err := json.Unmarshal(actualJSON, actual); err != nil {
		t.Fatalf("decode product result: %v", err)
	}
	assertEqualJSON(t, expected, actual)
}

func assertEqualJSON(t *testing.T, expected any, actual any) {
	t.Helper()
	if reflect.DeepEqual(expected, actual) {
		return
	}
	expectedJSON, expectedErr := json.MarshalIndent(expected, "", "  ")
	actualJSON, actualErr := json.MarshalIndent(actual, "", "  ")
	if expectedErr != nil || actualErr != nil {
		t.Fatal(fmt.Errorf("marshal mismatch: %v", errorsJoin(expectedErr, actualErr)))
	}
	t.Fatalf("merge result mismatch\nexpected:\n%s\nactual:\n%s", expectedJSON, actualJSON)
}

func errorsJoin(first error, second error) error {
	if first != nil {
		return first
	}
	return second
}
