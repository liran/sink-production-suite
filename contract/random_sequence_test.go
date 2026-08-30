package contract_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/liran/sink-production-suite/internal/fixture"
	"github.com/liran/sink-production-suite/internal/luatest"
	"github.com/liran/sink-production-suite/internal/reference"
	"github.com/liran/sink-production-suite/programs"
)

const (
	deterministicSequenceSeeds = 16
	deterministicSequenceSteps = 24
	maximumFuzzSequenceSteps   = 64
)

type sequenceOptions struct {
	seed       int64
	operations int
}

type sequencePosition struct {
	seed int64
	step int
}

type valueDifference struct {
	path     string
	expected any
	actual   any
}

type modelGenerator struct {
	random *rand.Rand
}

func TestRandomProductMergeSequencesMatchReferenceModel(t *testing.T) {
	for seed := int64(0); seed < deterministicSequenceSeeds; seed++ {
		options := sequenceOptions{seed: seed, operations: deterministicSequenceSteps}
		runProductMergeSequence(t, options)
	}
}

func TestRandomOfferMergeSequencesMatchReferenceModel(t *testing.T) {
	for seed := int64(0); seed < deterministicSequenceSeeds; seed++ {
		options := sequenceOptions{seed: seed, operations: deterministicSequenceSteps}
		runOfferMergeSequence(t, options)
	}
}

func FuzzProductMergeSequence(f *testing.F) {
	addSequenceSeeds(f)
	f.Fuzz(func(t *testing.T, seed int64, rawOperations uint8) {
		operations := int(rawOperations%maximumFuzzSequenceSteps) + 1
		options := sequenceOptions{seed: seed, operations: operations}
		runProductMergeSequence(t, options)
	})
}

func FuzzOfferMergeSequence(f *testing.F) {
	addSequenceSeeds(f)
	f.Fuzz(func(t *testing.T, seed int64, rawOperations uint8) {
		operations := int(rawOperations%maximumFuzzSequenceSteps) + 1
		options := sequenceOptions{seed: seed, operations: operations}
		runOfferMergeSequence(t, options)
	})
}

func addSequenceSeeds(f *testing.F) {
	f.Helper()
	seeds := []int64{0, 1, -1, 42, 9_007_199_254_740_993}
	for _, seed := range seeds {
		f.Add(seed, uint8(deterministicSequenceSteps))
	}
}

func runProductMergeSequence(t testing.TB, options sequenceOptions) {
	t.Helper()
	generator := newModelGenerator(options.seed)
	engine, err := luatest.New(programs.ProductMerge)
	if err != nil {
		t.Fatalf("seed %d: luatest.New(product) error = %v", options.seed, err)
	}
	defer engine.Close()

	var expected *reference.Product
	var actual *reference.Product
	if generator.chance(2) {
		expected = generator.product(-1)
		actual = expected
	}
	for step := range options.operations {
		incoming := generator.product(step)
		observedAt := fixture.BaseTime.Add(time.Duration(step+1) * time.Second)
		expected = reference.MergeProduct(expected, incoming, observedAt)
		actualJSON, mergeErr := engine.Merge(actual, incoming, observedAt)
		if mergeErr != nil {
			t.Fatalf("seed %d step %d: product Merge() error = %v", options.seed, step, mergeErr)
		}
		position := sequencePosition{seed: options.seed, step: step}
		actual = decodeSequenceResult[reference.Product](t, position, actualJSON)
		assertSequenceEqual(t, position, expected, actual)
		if step%6 == 0 {
			replayedExpected := reference.MergeProduct(expected, incoming, observedAt)
			replayedJSON, replayErr := engine.Merge(actual, incoming, observedAt)
			if replayErr != nil {
				t.Fatalf("seed %d step %d: replay product Merge() error = %v", options.seed, step, replayErr)
			}
			actual = decodeSequenceResult[reference.Product](t, position, replayedJSON)
			assertSequenceEqual(t, position, replayedExpected, actual)
			assertProductReplayInvariants(t, position, actual)
			expected = replayedExpected
		}
	}
}

func assertProductReplayInvariants(t testing.TB, position sequencePosition, product *reference.Product) {
	t.Helper()
	if len(product.Comments) > 20 {
		t.Fatalf("seed %d step %d: replay comments length = %d, want <= 20", position.seed, position.step, len(product.Comments))
	}
	if len(product.Offers) > 50 {
		t.Fatalf("seed %d step %d: replay offers length = %d, want <= 50", position.seed, position.step, len(product.Offers))
	}
	if len(product.Solds) > 20 {
		t.Fatalf("seed %d step %d: replay solds length = %d, want <= 20", position.seed, position.step, len(product.Solds))
	}
	if len(product.Stocks) > 20 {
		t.Fatalf("seed %d step %d: replay stocks length = %d, want <= 20", position.seed, position.step, len(product.Stocks))
	}

	uidKey := duplicateKey(product.UIDs, identityString)
	assertNoReplayDuplicate(t, position, "uids", uidKey)
	soldKey := duplicateKey(product.Solds, productSoldReplayKey)
	assertNoReplayDuplicate(t, position, "solds", soldKey)
	stockKey := duplicateKey(product.Stocks, productStockReplayKey)
	assertNoReplayDuplicate(t, position, "stocks", stockKey)
	commentKey := duplicateKey(product.Comments, replayJSONKey[*reference.ProductComment])
	assertNoReplayDuplicate(t, position, "comments", commentKey)
	offerKey := duplicateKey(product.Offers, func(value *reference.ProductOffer) string {
		return value.UID
	})
	assertNoReplayDuplicate(t, position, "offers", offerKey)
}

func assertNoReplayDuplicate[K comparable](t testing.TB, position sequencePosition, path string, key *K) {
	t.Helper()
	if key == nil {
		return
	}
	t.Fatalf("seed %d step %d: replay %s contains duplicate key %#v", position.seed, position.step, path, *key)
}

func duplicateKey[T any, K comparable](items []T, key func(T) K) *K {
	seen := make(map[K]struct{}, len(items))
	for _, item := range items {
		itemKey := key(item)
		if _, exists := seen[itemKey]; exists {
			duplicate := itemKey
			return &duplicate
		}
		seen[itemKey] = struct{}{}
	}
	return nil
}

func productSoldReplayKey(item *reference.ProductSold) string {
	recordAt := ""
	if item.RecordAt != nil {
		recordAt = item.RecordAt.Format("2006-01-02")
	}
	return fmt.Sprintf("%d-%d-%s", item.Sold, item.PeriodHours, recordAt)
}

func productStockReplayKey(item *reference.ProductStock) string {
	key := fmt.Sprintf("%d", item.Stock)
	if len(item.Variables) > 0 {
		key += replayJSONKey(item.Variables)
	}
	return key
}

func replayJSONKey[T any](value T) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("encode replay key: %v", err))
	}
	return string(encoded)
}

func runOfferMergeSequence(t testing.TB, options sequenceOptions) {
	t.Helper()
	generator := newModelGenerator(options.seed)
	engine, err := luatest.New(programs.OfferMerge)
	if err != nil {
		t.Fatalf("seed %d: luatest.New(offer) error = %v", options.seed, err)
	}
	defer engine.Close()

	var expected *reference.Offer
	var actual *reference.Offer
	if generator.chance(2) {
		expected = generator.offer(-1)
		actual = expected
	}
	for step := range options.operations {
		incoming := generator.offer(step)
		observedAt := fixture.BaseTime.Add(time.Duration(step+1) * time.Second)
		expected = reference.MergeOffer(expected, incoming, observedAt)
		actualJSON, mergeErr := engine.Merge(actual, incoming, observedAt)
		if mergeErr != nil {
			t.Fatalf("seed %d step %d: offer Merge() error = %v", options.seed, step, mergeErr)
		}
		position := sequencePosition{seed: options.seed, step: step}
		actual = decodeSequenceResult[reference.Offer](t, position, actualJSON)
		assertSequenceEqual(t, position, expected, actual)
		if step%6 == 0 {
			replayedExpected := reference.MergeOffer(expected, incoming, observedAt)
			replayedJSON, replayErr := engine.Merge(actual, incoming, observedAt)
			if replayErr != nil {
				t.Fatalf("seed %d step %d: replay offer Merge() error = %v", options.seed, step, replayErr)
			}
			actual = decodeSequenceResult[reference.Offer](t, position, replayedJSON)
			assertSequenceEqual(t, position, replayedExpected, actual)
			assertOfferReplayInvariants(t, position, actual)
			expected = replayedExpected
		}
	}
}

func assertOfferReplayInvariants(t testing.TB, position sequencePosition, offer *reference.Offer) {
	t.Helper()
	if len(offer.Addresses) > 10 {
		t.Fatalf("seed %d step %d: replay addresses length = %d, want <= 10", position.seed, position.step, len(offer.Addresses))
	}

	uidKey := duplicateKey(offer.UIDs, identityString)
	assertNoReplayDuplicate(t, position, "uids", uidKey)
	addressKey := duplicateKey(offer.Addresses, offerAddressReplayKey)
	assertNoReplayDuplicate(t, position, "addresses", addressKey)
	contactKey := duplicateKey(offer.Contacts, offerContactReplayKey)
	assertNoReplayDuplicate(t, position, "contacts", contactKey)
	assertTrackingReplayInvariants(t, position, offer.TrackingIDs)
}

func assertTrackingReplayInvariants(t testing.TB, position sequencePosition, tracking *reference.TrackingIDs) {
	t.Helper()
	if tracking == nil {
		return
	}
	fields := map[string][]string{
		"tracking_ids.adsense":              tracking.GoogleAdSense,
		"tracking_ids.bing":                 tracking.BingSiteVerification,
		"tracking_ids.facebook_domain":      tracking.FacebookDomainVerification,
		"tracking_ids.facebook_pixel":       tracking.FacebookPixel,
		"tracking_ids.ga4":                  tracking.GoogleAnalyticsGA4,
		"tracking_ids.ga_ua":                tracking.GoogleAnalyticsUA,
		"tracking_ids.gtm":                  tracking.GoogleTagManager,
		"tracking_ids.hotjar":               tracking.Hotjar,
		"tracking_ids.inline_script_hashes": tracking.InlineScriptHashes,
		"tracking_ids.pinterest_domain":     tracking.PinterestDomainVerify,
		"tracking_ids.script_urls":          tracking.ScriptURLs,
		"tracking_ids.yandex_metrica":       tracking.YandexMetrica,
		"tracking_ids.yandex_verification":  tracking.YandexVerification,
	}
	paths := make([]string, 0, len(fields))
	for path := range fields {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		key := duplicateKey(fields[path], identityString)
		assertNoReplayDuplicate(t, position, path, key)
	}
}

func identityString(value string) string {
	return value
}

func offerAddressReplayKey(item *reference.Address) string {
	return item.Raw + item.Country
}

func offerContactReplayKey(item *reference.Contact) string {
	return item.Type + item.Value
}

func decodeSequenceResult[T any](t testing.TB, position sequencePosition, encoded []byte) *T {
	t.Helper()
	result := new(T)
	err := json.Unmarshal(encoded, result)
	if err != nil {
		t.Fatalf("seed %d step %d: decode merge result: %v\n%s", position.seed, position.step, err, encoded)
	}
	return result
}

func assertSequenceEqual(t testing.TB, position sequencePosition, expected any, actual any) {
	t.Helper()
	if reflect.DeepEqual(expected, actual) {
		return
	}
	difference := firstValueDifference(reflect.ValueOf(expected), reflect.ValueOf(actual), "$")
	if difference == nil {
		t.Fatalf("seed %d step %d: merge results differ without a diagnostic", position.seed, position.step)
	}
	t.Fatalf(
		"seed %d step %d: merge mismatch at %s: expected=%#v actual=%#v",
		position.seed,
		position.step,
		difference.path,
		difference.expected,
		difference.actual,
	)
}

func firstValueDifference(expected reflect.Value, actual reflect.Value, path string) *valueDifference {
	if !expected.IsValid() || !actual.IsValid() {
		if expected.IsValid() == actual.IsValid() {
			return nil
		}
		return newValueDifference(path, reflectionValue(expected), reflectionValue(actual))
	}
	if expected.Type() != actual.Type() {
		return newValueDifference(path, expected.Type(), actual.Type())
	}
	if expected.Type() == reflect.TypeFor[time.Time]() {
		if reflect.DeepEqual(expected.Interface(), actual.Interface()) {
			return nil
		}
		return newValueDifference(path, expected.Interface(), actual.Interface())
	}
	if expected.Kind() == reflect.Interface || expected.Kind() == reflect.Pointer {
		if expected.IsNil() || actual.IsNil() {
			if expected.IsNil() == actual.IsNil() {
				return nil
			}
			return newValueDifference(path, reflectionValue(expected), reflectionValue(actual))
		}
		return firstValueDifference(expected.Elem(), actual.Elem(), path)
	}

	switch expected.Kind() {
	case reflect.Struct:
		for index := 0; index < expected.NumField(); index++ {
			fieldPath := path + "." + expected.Type().Field(index).Name
			difference := firstValueDifference(expected.Field(index), actual.Field(index), fieldPath)
			if difference != nil {
				return difference
			}
		}
	case reflect.Slice, reflect.Array:
		if expected.Len() != actual.Len() {
			return newValueDifference(path+".length", expected.Len(), actual.Len())
		}
		for index := 0; index < expected.Len(); index++ {
			itemPath := fmt.Sprintf("%s[%d]", path, index)
			difference := firstValueDifference(expected.Index(index), actual.Index(index), itemPath)
			if difference != nil {
				return difference
			}
		}
	case reflect.Map:
		if expected.Len() != actual.Len() {
			return newValueDifference(path+".length", expected.Len(), actual.Len())
		}
		keys := expected.MapKeys()
		sort.Slice(keys, func(left int, right int) bool {
			return fmt.Sprint(keys[left].Interface()) < fmt.Sprint(keys[right].Interface())
		})
		for _, key := range keys {
			expectedItem := expected.MapIndex(key)
			actualItem := actual.MapIndex(key)
			itemPath := fmt.Sprintf("%s[%v]", path, key.Interface())
			difference := firstValueDifference(expectedItem, actualItem, itemPath)
			if difference != nil {
				return difference
			}
		}
	default:
		if !reflect.DeepEqual(expected.Interface(), actual.Interface()) {
			return newValueDifference(path, expected.Interface(), actual.Interface())
		}
	}
	return nil
}

func newValueDifference(path string, expected any, actual any) *valueDifference {
	difference := &valueDifference{path: path, expected: expected, actual: actual}
	return difference
}

func reflectionValue(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
	}
	return value.Interface()
}

func newModelGenerator(seed int64) *modelGenerator {
	randomSource := rand.NewSource(seed)
	generator := &modelGenerator{random: rand.New(randomSource)}
	return generator
}

func (g *modelGenerator) chance(oneIn int) bool {
	return g.random.Intn(oneIn) == 0
}

func (g *modelGenerator) token(prefix string) string {
	if g.chance(4) {
		return ""
	}
	values := []string{"alpha", "beta", "café", "Straße", "品牌", "line\nbreak", "quote\"value"}
	value := values[g.random.Intn(len(values))]
	return fmt.Sprintf("%s-%s-%d", prefix, value, g.random.Intn(6))
}

func (g *modelGenerator) strings(prefix string, maximum int) []string {
	count := g.random.Intn(maximum + 1)
	if count == 0 {
		return nil
	}
	result := make([]string, 0, count)
	for index := 0; index < count; index++ {
		value := g.token(prefix)
		if len(result) > 0 && g.chance(3) {
			value = result[g.random.Intn(len(result))]
		}
		result = append(result, value)
	}
	return result
}

func (g *modelGenerator) integer() int64 {
	values := []int64{0, 1, -1, 19, 20, 21, 49, 50, 51, 9_007_199_254_740_993}
	return values[g.random.Intn(len(values))]
}

func (g *modelGenerator) number() float64 {
	values := []float64{0, -1.25, 0.5, 1, 19.75, 9_007_199_254_740_992}
	return values[g.random.Intn(len(values))]
}

func (g *modelGenerator) timestamp(step int) *time.Time {
	if g.chance(4) {
		return nil
	}
	offset := time.Duration(step*17+g.random.Intn(17)) * time.Minute
	stamp := fixture.BaseTime.Add(offset).UTC()
	return &stamp
}

func (g *modelGenerator) variables() map[string]any {
	if g.chance(3) {
		return nil
	}
	variables := make(map[string]any)
	variables["size"] = g.token("size")
	variables["slot"] = g.random.Intn(8)
	variables["flags"] = []any{g.chance(2), g.token("flag")}
	return variables
}

func (g *modelGenerator) product(step int) *reference.Product {
	product := &reference.Product{
		UID:                 g.token("product-uid"),
		UIDs:                g.strings("product-alias", 4),
		Platform:            g.token("platform"),
		Country:             g.token("country"),
		ID:                  g.token("product-id"),
		URL:                 g.token("product-url"),
		Title:               g.token("title"),
		Description:         g.token("description"),
		Condition:           g.token("condition"),
		Category:            g.strings("category", 4),
		Gallery:             g.images(),
		Brand:               g.token("brand"),
		Solds:               g.solds(step),
		Prices:              g.prices(step),
		Stocks:              g.stocks(),
		CommentCount:        g.integer(),
		Comments:            g.comments(step),
		Rating:              g.rating(),
		Offers:              g.productOffers(step),
		AllowedCountries:    g.strings("allowed", 4),
		RestrictedCountries: g.strings("restricted", 4),
		SerialNumber:        g.token("serial"),
		Languages:           g.strings("language", 4),
		CountriesFromIP:     g.strings("ip-country", 4),
		SoldByPlatform:      g.chance(2),
		FromMaybe:           g.chance(2),
		Available:           g.chance(2),
		FirstFoundAt:        g.timestamp(step - 100),
		LastFoundAt:         g.timestamp(step - 50),
		EvictedAt:           g.timestamp(step - 10),
		Hostnames:           g.strings("hostname", 3),
		EcommerceClass:      g.token("ecommerce-class"),
		PtoClass:            g.ptoClasses(),
		Brands:              g.strings("brands", 3),
		TranslatedText:      g.token("translated"),
	}
	return product
}

func (g *modelGenerator) images() []*reference.Image {
	count := g.random.Intn(4)
	result := make([]*reference.Image, 0, count)
	for range count {
		image := &reference.Image{URL: g.token("image-url"), Key: g.token("image-key")}
		result = append(result, image)
	}
	return result
}

func (g *modelGenerator) solds(step int) []*reference.ProductSold {
	count := g.random.Intn(5)
	result := make([]*reference.ProductSold, 0, count)
	for index := 0; index < count; index++ {
		sold := &reference.ProductSold{
			Sold:        int64(g.random.Intn(16)),
			PeriodHours: int64([]int{0, 1, 24, 168}[g.random.Intn(4)]),
			RecordAt:    g.timestamp(step + index),
		}
		if len(result) > 0 && g.chance(3) {
			sold = result[g.random.Intn(len(result))]
		}
		result = append(result, sold)
	}
	return result
}

func (g *modelGenerator) prices(step int) []*reference.ProductPrice {
	count := g.random.Intn(4)
	result := make([]*reference.ProductPrice, 0, count)
	for index := 0; index < count; index++ {
		price := &reference.ProductPrice{
			Currency:  g.token("currency"),
			Price:     g.number(),
			Dollar:    g.number(),
			Variables: g.variables(),
			Date:      g.timestamp(step + index),
		}
		result = append(result, price)
	}
	return result
}

func (g *modelGenerator) stocks() []*reference.ProductStock {
	count := g.random.Intn(5)
	result := make([]*reference.ProductStock, 0, count)
	for range count {
		stock := &reference.ProductStock{Stock: int64(g.random.Intn(16)), Variables: g.variables()}
		if len(result) > 0 && g.chance(3) {
			stock = result[g.random.Intn(len(result))]
		}
		result = append(result, stock)
	}
	return result
}

func (g *modelGenerator) comments(step int) []*reference.ProductComment {
	count := g.random.Intn(5)
	result := make([]*reference.ProductComment, 0, count)
	for index := 0; index < count; index++ {
		comment := &reference.ProductComment{
			Score:               g.number(),
			Title:               g.token("comment-title"),
			Description:         g.token("comment-description"),
			CustomerName:        g.token("customer-name"),
			CustomerProfile:     g.token("customer-profile"),
			CustomerRatingScore: g.number(),
			Location:            g.token("location"),
			Currency:            g.token("comment-currency"),
			Price:               g.number(),
			Date:                g.timestamp(step + index),
		}
		if len(result) > 0 && g.chance(3) {
			comment = result[g.random.Intn(len(result))]
		}
		result = append(result, comment)
	}
	return result
}

func (g *modelGenerator) rating() *reference.Rating {
	if g.chance(3) {
		return nil
	}
	rating := &reference.Rating{Score: g.number(), Count: g.integer(), Description: g.token("rating")}
	return rating
}

func (g *modelGenerator) productOffers(step int) []*reference.ProductOffer {
	count := g.random.Intn(6)
	result := make([]*reference.ProductOffer, 0, count)
	for index := 0; index < count; index++ {
		offer := &reference.ProductOffer{
			ID:               g.token("seller-id"),
			UID:              fmt.Sprintf("seller-%d", g.random.Intn(12)),
			UIDs:             g.strings("seller-alias", 3),
			URL:              g.token("seller-url"),
			Name:             g.token("seller-name"),
			ExpiryDetectedAt: g.timestamp(step + index),
			ProductURL:       g.token("seller-product-url"),
			ProductPrice:     g.prices(step + index),
			HasPayPal:        g.chance(2),
		}
		result = append(result, offer)
	}
	return result
}

func (g *modelGenerator) ptoClasses() []*reference.PtoClass {
	count := g.random.Intn(4)
	result := make([]*reference.PtoClass, 0, count)
	for range count {
		class := &reference.PtoClass{
			ClassCode:        g.token("class-code"),
			GoodsCode:        g.token("goods-code"),
			GoodsDescription: g.token("goods-description"),
		}
		result = append(result, class)
	}
	return result
}

func (g *modelGenerator) offer(step int) *reference.Offer {
	offer := &reference.Offer{
		UID:           g.token("offer-uid"),
		UIDs:          g.strings("offer-alias", 4),
		Platform:      g.token("offer-platform"),
		ID:            g.token("offer-id"),
		Name:          g.token("offer-name"),
		URL:           g.token("offer-url"),
		Country:       g.token("offer-country"),
		BusinessName:  g.token("business-name"),
		Addresses:     g.addresses(),
		Contacts:      g.contacts(),
		Bio:           g.token("bio"),
		MemberSince:   g.timestamp(step - 100),
		Sold:          g.integer(),
		CommentCount:  g.integer(),
		FollowerCount: g.integer(),
		ProductCount:  g.integer(),
		FromMaybe:     g.chance(2),
		FirstFoundAt:  g.timestamp(step - 90),
		LastFoundAt:   g.timestamp(step - 40),
		EvictedAt:     g.timestamp(step - 10),
		Hostnames:     g.strings("offer-hostname", 4),
		TrackingIDs:   g.trackingIDs(),
		HasPayPal:     g.chance(2),
	}
	return offer
}

func (g *modelGenerator) addresses() []*reference.Address {
	count := g.random.Intn(4)
	result := make([]*reference.Address, 0, count)
	for range count {
		address := &reference.Address{
			Raw:     fmt.Sprintf("address-%d", g.random.Intn(12)),
			Street:  g.token("street"),
			City:    g.token("city"),
			State:   g.token("state"),
			Country: fmt.Sprintf("country-%d", g.random.Intn(4)),
			ZipCode: g.token("zip"),
		}
		result = append(result, address)
	}
	return result
}

func (g *modelGenerator) contacts() []*reference.Contact {
	count := g.random.Intn(4)
	result := make([]*reference.Contact, 0, count)
	for range count {
		contact := &reference.Contact{
			Type:  fmt.Sprintf("type-%d", g.random.Intn(4)),
			Value: fmt.Sprintf("value-%d", g.random.Intn(12)),
		}
		result = append(result, contact)
	}
	return result
}

func (g *modelGenerator) trackingIDs() *reference.TrackingIDs {
	if g.chance(3) {
		return nil
	}
	tracking := &reference.TrackingIDs{
		GoogleAnalyticsUA:          g.strings("ga-ua", 3),
		GoogleAnalyticsGA4:         g.strings("ga4", 3),
		GoogleTagManager:           g.strings("gtm", 3),
		GoogleAdSense:              g.strings("adsense", 3),
		FacebookPixel:              g.strings("facebook-pixel", 3),
		Hotjar:                     g.strings("hotjar", 3),
		YandexMetrica:              g.strings("yandex-metrica", 3),
		FacebookDomainVerification: g.strings("facebook-domain", 3),
		PinterestDomainVerify:      g.strings("pinterest-domain", 3),
		YandexVerification:         g.strings("yandex-verification", 3),
		BingSiteVerification:       g.strings("bing", 3),
		ScriptURLs:                 g.strings("script-url", 3),
		InlineScriptHashes:         g.strings("script-hash", 3),
	}
	return tracking
}
