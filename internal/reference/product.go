package reference

import (
	"fmt"
	"strings"
	"time"
)

func MergeProduct(current *Product, incoming *Product, observedAt time.Time) *Product {
	result := &Product{}
	currentCopy := cloneJSON(current)
	if currentCopy != nil {
		result.MergeAt(observedAt, currentCopy)
	}
	incomingCopy := cloneJSON(incoming)
	result.MergeAt(observedAt, incomingCopy)
	result.EvictedAt = nil
	return result
}

func (p *Product) MergeAt(observedAt time.Time, products ...*Product) {
	for _, incoming := range products {
		p.UIDs = append(p.UIDs, p.UID)
		if incoming.UID != "" {
			p.UIDs = append(p.UIDs, incoming.UID)
			p.UID = incoming.UID
		}
		p.UIDs = append(p.UIDs, incoming.UIDs...)
		p.UIDs = uniqueStrings(p.UIDs, true)

		replaceString(&p.Platform, incoming.Platform)
		replaceString(&p.Country, incoming.Country)
		replaceString(&p.ID, incoming.ID)
		replaceString(&p.URL, incoming.URL)
		replaceString(&p.Title, incoming.Title)
		replaceString(&p.Description, incoming.Description)
		replaceString(&p.Condition, incoming.Condition)
		replaceSlice(&p.Category, incoming.Category)
		replaceSlice(&p.Gallery, incoming.Gallery)
		if incoming.Brand != "" {
			p.Brand = strings.ToUpper(incoming.Brand)
		}

		if len(incoming.Solds) > 0 {
			p.Solds = appendUnique(p.Solds, incoming.Solds, productSoldKey)
		}
		p.Solds = keepTail(p.Solds, 20)
		replaceSlice(&p.Prices, incoming.Prices)
		if len(incoming.Stocks) > 0 {
			p.Stocks = appendUnique(p.Stocks, incoming.Stocks, productStockKey)
		}
		p.Stocks = keepTail(p.Stocks, 20)

		if incoming.CommentCount != 0 {
			p.CommentCount = incoming.CommentCount
		}
		if len(incoming.Comments) > 0 {
			p.Comments = appendUnique(p.Comments, incoming.Comments, func(item *ProductComment) [20]byte {
				return jsonDigest(item)
			})
		}
		p.Comments = keepTail(p.Comments, 20)
		if incoming.Rating != nil {
			p.Rating = incoming.Rating
		}

		if len(incoming.Offers) > 0 {
			combined := append([]*ProductOffer(nil), incoming.Offers...)
			combined = append(combined, p.Offers...)
			p.Offers = appendUnique([]*ProductOffer(nil), combined, func(item *ProductOffer) string {
				return item.UID
			})
		}
		p.Offers = keepTail(p.Offers, 50)

		if len(incoming.AllowedCountries) > 0 {
			p.AllowedCountries = uniqueStrings(append(p.AllowedCountries, incoming.AllowedCountries...), false)
		}
		if len(incoming.RestrictedCountries) > 0 {
			p.RestrictedCountries = uniqueStrings(append(p.RestrictedCountries, incoming.RestrictedCountries...), false)
		}

		stamp := observedAt.UTC()
		p.LastFoundAt = &stamp
		if p.FirstFoundAt == nil {
			p.FirstFoundAt = incoming.FirstFoundAt
		}
		if p.FirstFoundAt == nil {
			first := stamp
			p.FirstFoundAt = &first
		}
		if incoming.EvictedAt != nil {
			p.EvictedAt = incoming.EvictedAt
		}

		replaceSlice(&p.Hostnames, incoming.Hostnames)
		replaceString(&p.EcommerceClass, incoming.EcommerceClass)
		replaceSlice(&p.PtoClass, incoming.PtoClass)
		replaceSlice(&p.Brands, incoming.Brands)
		replaceString(&p.TranslatedText, incoming.TranslatedText)
		if len(incoming.Languages) > 0 {
			p.Languages = uniqueStrings(append(p.Languages, incoming.Languages...), false)
		}
		if len(incoming.CountriesFromIP) > 0 {
			p.CountriesFromIP = uniqueStrings(append(p.CountriesFromIP, incoming.CountriesFromIP...), false)
		}
		replaceString(&p.SerialNumber, incoming.SerialNumber)
		p.SoldByPlatform = incoming.SoldByPlatform
		p.Available = incoming.Available
	}
}

func replaceString(target *string, incoming string) {
	if incoming != "" {
		*target = incoming
	}
}

func replaceSlice[T any](target *[]T, incoming []T) {
	if len(incoming) > 0 {
		*target = incoming
	}
}

func productSoldKey(item *ProductSold) string {
	recordAt := ""
	if item.RecordAt != nil {
		recordAt = item.RecordAt.Format("2006-01-02")
	}
	return fmt.Sprintf("%d-%d-%s", item.Sold, item.PeriodHours, recordAt)
}

func productStockKey(item *ProductStock) string {
	key := fmt.Sprintf("%d", item.Stock)
	if len(item.Variables) > 0 {
		digest := jsonDigest(item.Variables)
		key += fmt.Sprintf("%x", digest)
	}
	return key
}
