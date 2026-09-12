//go:build integration

package conformance_test

import "testing"

func TestFreeAddressesAreDistinct(t *testing.T) {
	addresses := freeAddresses(t, 2)
	if addresses[0] == addresses[1] {
		t.Fatalf("free addresses must be distinct: %q", addresses[0])
	}
}
