package reference

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
)

func appendUnique[T any, K comparable](current []T, incoming []T, key func(T) K) []T {
	combined := append(current, incoming...)
	result := make([]T, 0, len(combined))
	seen := make(map[K]struct{}, len(combined))
	for _, item := range combined {
		itemKey := key(item)
		if _, exists := seen[itemKey]; exists {
			continue
		}
		seen[itemKey] = struct{}{}
		result = append(result, item)
	}
	return result
}

func uniqueStrings(items []string, removeEmpty bool) []string {
	result := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if removeEmpty && item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	return result
}

func keepTail[T any](items []T, limit int) []T {
	if len(items) <= limit {
		return items
	}
	return items[len(items)-limit:]
}

func jsonDigest(value any) [sha1.Size]byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal reference hash input: %v", err))
	}
	return sha1.Sum(encoded)
}

func cloneJSON[T any](value *T) *T {
	if value == nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal reference clone: %v", err))
	}
	result := new(T)
	if err := json.Unmarshal(encoded, result); err != nil {
		panic(fmt.Sprintf("unmarshal reference clone: %v", err))
	}
	return result
}
