// Package fixture provides stable, representative product-write documents.
package fixture

import (
	"fmt"
	"time"

	"github.com/liran/sink-production-suite/internal/reference"
)

var BaseTime = time.Date(2026, time.August, 29, 8, 30, 0, 123000000, time.UTC)

func ProductPair() (*reference.Product, *reference.Product) {
	currentPriceDate := BaseTime.Add(-48 * time.Hour)
	currentFound := BaseTime.Add(-72 * time.Hour)
	evicted := BaseTime.Add(-time.Hour)
	current := &reference.Product{
		UID:                 "shopify:example.com:100",
		UIDs:                []string{"legacy:100", "shopify:example.com:100"},
		Platform:            "shopify",
		Country:             "US",
		ID:                  "100",
		URL:                 "https://example.com/products/100",
		Title:               "Old title",
		Description:         "Old description",
		Condition:           "used",
		Category:            []string{"old"},
		Gallery:             []*reference.Image{{URL: "https://images.example/old.jpg", Key: "old"}},
		Brand:               "OLD BRAND",
		Solds:               []*reference.ProductSold{{Sold: 10, PeriodHours: 24, RecordAt: &currentPriceDate}},
		Prices:              []*reference.ProductPrice{{Currency: "USD", Price: 10, Date: &currentPriceDate}},
		Stocks:              []*reference.ProductStock{{Stock: 4, Variables: map[string]any{"size": "M", "color": "black"}}},
		CommentCount:        5,
		Comments:            []*reference.ProductComment{{Score: 4, Title: "old", CustomerName: "a"}},
		Rating:              &reference.Rating{Score: 4, Count: 5},
		Offers:              []*reference.ProductOffer{{ID: "seller-a", UID: "shopify:seller-a", URL: "https://seller-a.example"}},
		AllowedCountries:    []string{"US", "CA"},
		RestrictedCountries: []string{"CN"},
		SerialNumber:        "OLD-SERIAL",
		Languages:           []string{"en"},
		CountriesFromIP:     []string{"US"},
		SoldByPlatform:      true,
		FromMaybe:           true,
		Available:           true,
		FirstFoundAt:        &currentFound,
		LastFoundAt:         &currentFound,
		EvictedAt:           &evicted,
		Hostnames:           []string{"old.example"},
		EcommerceClass:      "old-class",
		PtoClass:            []*reference.PtoClass{{ClassCode: "01", GoodsCode: "0101"}},
		Brands:              []string{"OLD"},
		TranslatedText:      "old translation",
	}

	incomingPriceDate := BaseTime.Add(-24 * time.Hour)
	incoming := &reference.Product{
		UID:                 "shopify:example.com:100",
		UIDs:                []string{"alias:100", "legacy:100"},
		Platform:            "shopify",
		Country:             "GB",
		ID:                  "100",
		URL:                 "https://example.com/products/100?new=1",
		Title:               "New title",
		Description:         "New description",
		Condition:           "new",
		Category:            []string{"fashion", "bags"},
		Gallery:             []*reference.Image{{URL: "https://images.example/new.jpg", Key: "new"}},
		Brand:               "new brand",
		Solds:               []*reference.ProductSold{{Sold: 11, PeriodHours: 24, RecordAt: &incomingPriceDate}},
		Prices:              []*reference.ProductPrice{{Currency: "USD", Price: 12.5, Dollar: 12.5, Date: &incomingPriceDate}},
		Stocks:              []*reference.ProductStock{{Stock: 8, Variables: map[string]any{"color": "blue", "size": "L"}}},
		CommentCount:        6,
		Comments:            []*reference.ProductComment{{Score: 5, Title: "new", CustomerName: "b"}},
		Rating:              &reference.Rating{Score: 4.5, Count: 6, Description: "good"},
		Offers:              []*reference.ProductOffer{{ID: "seller-b", UID: "shopify:seller-b", URL: "https://seller-b.example"}},
		AllowedCountries:    []string{"US", "JP"},
		RestrictedCountries: []string{"RU"},
		SerialNumber:        "NEW-SERIAL",
		Languages:           []string{"en", "ja"},
		CountriesFromIP:     []string{"JP"},
		SoldByPlatform:      false,
		Available:           false,
		Hostnames:           []string{"new.example"},
		EcommerceClass:      "new-class",
		PtoClass:            []*reference.PtoClass{{ClassCode: "02", GoodsCode: "0202"}},
		Brands:              []string{"NEW"},
		TranslatedText:      "new translation",
	}
	return current, incoming
}

func ProductHistoryPair() (*reference.Product, *reference.Product) {
	current, incoming := ProductPair()
	current.Solds = nil
	current.Stocks = nil
	current.Comments = nil
	incoming.Solds = nil
	incoming.Stocks = nil
	incoming.Comments = nil
	for index := range 15 {
		stamp := BaseTime.Add(time.Duration(index-30) * 24 * time.Hour)
		sold := &reference.ProductSold{Sold: int64(index), PeriodHours: 24, RecordAt: &stamp}
		stock := &reference.ProductStock{Stock: int64(index), Variables: map[string]any{"size": index}}
		comment := &reference.ProductComment{Score: float64(index), Title: fmt.Sprintf("current-%d", index)}
		current.Solds = append(current.Solds, sold)
		current.Stocks = append(current.Stocks, stock)
		current.Comments = append(current.Comments, comment)
	}
	for index := 10; index < 30; index++ {
		stamp := BaseTime.Add(time.Duration(index-30) * 24 * time.Hour)
		sold := &reference.ProductSold{Sold: int64(index), PeriodHours: 24, RecordAt: &stamp}
		stock := &reference.ProductStock{Stock: int64(index), Variables: map[string]any{"size": index}}
		comment := &reference.ProductComment{Score: float64(index), Title: fmt.Sprintf("incoming-%d", index)}
		incoming.Solds = append(incoming.Solds, sold)
		incoming.Stocks = append(incoming.Stocks, stock)
		incoming.Comments = append(incoming.Comments, comment)
	}
	return current, incoming
}

func OfferPair() (*reference.Offer, *reference.Offer) {
	memberSince := BaseTime.Add(-365 * 24 * time.Hour)
	firstFound := BaseTime.Add(-72 * time.Hour)
	evicted := BaseTime.Add(-time.Hour)
	current := &reference.Offer{
		UID:           "shopify:example.com",
		UIDs:          []string{"legacy:example.com"},
		Platform:      "shopify",
		ID:            "example.com",
		Name:          "Old shop",
		URL:           "https://example.com",
		Country:       "US",
		BusinessName:  "Old LLC",
		Addresses:     []*reference.Address{{Raw: "1 Main St", Country: "US"}},
		Contacts:      []*reference.Contact{{Type: "email", Value: "old@example.com"}},
		Bio:           "old bio",
		MemberSince:   &memberSince,
		Sold:          10,
		CommentCount:  9,
		FollowerCount: 20,
		ProductCount:  30,
		FromMaybe:     true,
		FirstFoundAt:  &firstFound,
		LastFoundAt:   &firstFound,
		EvictedAt:     &evicted,
		Hostnames:     []string{"old.example.com"},
		TrackingIDs: &reference.TrackingIDs{
			GoogleAnalyticsUA:  []string{"UA-OLD"},
			GoogleAnalyticsGA4: []string{"G-OLD"},
			ScriptURLs:         []string{"https://cdn.example/old.js"},
		},
		HasPayPal: true,
	}
	incoming := &reference.Offer{
		UID:           "shopify:example.com",
		UIDs:          []string{"alias:example.com", "legacy:example.com"},
		Platform:      "shopify",
		ID:            "example.com",
		Name:          "New shop",
		URL:           "https://www.example.com",
		Country:       "GB",
		BusinessName:  "New Ltd",
		Addresses:     []*reference.Address{{Raw: "2 High St", Country: "GB"}},
		Contacts:      []*reference.Contact{{Type: "phone", Value: "+44-100"}},
		Bio:           "new bio",
		Sold:          11,
		FollowerCount: 21,
		ProductCount:  31,
		Hostnames:     []string{"new.example.com"},
		TrackingIDs: &reference.TrackingIDs{
			GoogleAnalyticsUA:          []string{"UA-OLD", "UA-NEW"},
			GoogleAnalyticsGA4:         []string{"G-NEW"},
			GoogleTagManager:           []string{"GTM-NEW"},
			GoogleAdSense:              []string{"pub-new"},
			FacebookPixel:              []string{"pixel-new"},
			Hotjar:                     []string{"hotjar-new"},
			YandexMetrica:              []string{"yandex-new"},
			FacebookDomainVerification: []string{"facebook-domain-new"},
			PinterestDomainVerify:      []string{"pinterest-new"},
			YandexVerification:         []string{"yandex-verification-new"},
			BingSiteVerification:       []string{"bing-new"},
			ScriptURLs:                 []string{"https://cdn.example/new.js"},
			InlineScriptHashes:         []string{"sha256-new"},
		},
		HasPayPal: false,
	}
	return current, incoming
}

func OfferAddressHistoryPair() (*reference.Offer, *reference.Offer) {
	current, incoming := OfferPair()
	current.Addresses = nil
	incoming.Addresses = nil
	for index := range 8 {
		address := &reference.Address{Raw: fmt.Sprintf("current-%d", index), Country: "US"}
		current.Addresses = append(current.Addresses, address)
	}
	for index := 5; index < 15; index++ {
		address := &reference.Address{Raw: fmt.Sprintf("incoming-%d", index), Country: "US"}
		incoming.Addresses = append(incoming.Addresses, address)
	}
	return current, incoming
}

func RepresentativeProduct(index int) *reference.Product {
	current, incoming := ProductPair()
	_ = current
	uid := fmt.Sprintf("shopify:load.example:%d", index)
	incoming.UID = uid
	incoming.UIDs = []string{uid}
	incoming.ID = fmt.Sprintf("%d", index)
	incoming.URL = fmt.Sprintf("https://load.example/products/%d", index)
	incoming.Title = fmt.Sprintf("Representative product %d", index)
	incoming.Description = incoming.Description + " with a stable production qualification payload"
	return incoming
}
