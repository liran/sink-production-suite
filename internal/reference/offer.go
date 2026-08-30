package reference

import (
	"fmt"
	"time"
)

func MergeOffer(current *Offer, incoming *Offer, observedAt time.Time) *Offer {
	result := &Offer{}
	currentCopy := cloneJSON(current)
	if currentCopy != nil {
		result.MergeAt(observedAt, currentCopy)
	}
	incomingCopy := cloneJSON(incoming)
	result.MergeAt(observedAt, incomingCopy)
	result.EvictedAt = nil
	return result
}

func (o *Offer) MergeAt(observedAt time.Time, offers ...*Offer) {
	for _, incoming := range offers {
		replaceString(&o.UID, incoming.UID)
		if len(incoming.UIDs) > 0 {
			o.UIDs = uniqueStrings(append(o.UIDs, incoming.UIDs...), false)
		}
		replaceString(&o.Platform, incoming.Platform)
		replaceString(&o.ID, incoming.ID)
		replaceString(&o.Name, incoming.Name)
		replaceString(&o.URL, incoming.URL)
		replaceString(&o.Country, incoming.Country)
		replaceString(&o.BusinessName, incoming.BusinessName)

		if len(incoming.Addresses) > 0 {
			o.Addresses = appendUnique(o.Addresses, incoming.Addresses, func(item *Address) string {
				return item.Raw + item.Country
			})
		}
		o.Addresses = keepTail(o.Addresses, 10)
		if len(incoming.Contacts) > 0 {
			o.Contacts = appendUnique(o.Contacts, incoming.Contacts, func(item *Contact) string {
				return fmt.Sprintf("%s%s", item.Type, item.Value)
			})
		}

		replaceString(&o.Bio, incoming.Bio)
		if incoming.MemberSince != nil {
			o.MemberSince = incoming.MemberSince
		}
		if incoming.Sold != 0 {
			o.Sold = incoming.Sold
		}
		if incoming.FollowerCount != 0 {
			o.FollowerCount = incoming.FollowerCount
		}
		if incoming.ProductCount != 0 {
			o.ProductCount = incoming.ProductCount
		}

		stamp := observedAt.UTC()
		o.LastFoundAt = &stamp
		if o.FirstFoundAt == nil {
			o.FirstFoundAt = incoming.FirstFoundAt
		}
		if o.FirstFoundAt == nil {
			first := stamp
			o.FirstFoundAt = &first
		}
		if incoming.EvictedAt != nil {
			o.EvictedAt = incoming.EvictedAt
		}
		replaceSlice(&o.Hostnames, incoming.Hostnames)
		mergeTrackingIDs(o, incoming.TrackingIDs)
		o.HasPayPal = incoming.HasPayPal
	}
}

func mergeTrackingIDs(offer *Offer, incoming *TrackingIDs) {
	if incoming == nil {
		return
	}
	if offer.TrackingIDs == nil {
		offer.TrackingIDs = &TrackingIDs{}
	}
	mergeTrackingField(&offer.TrackingIDs.GoogleAnalyticsUA, incoming.GoogleAnalyticsUA)
	mergeTrackingField(&offer.TrackingIDs.GoogleAnalyticsGA4, incoming.GoogleAnalyticsGA4)
	mergeTrackingField(&offer.TrackingIDs.GoogleTagManager, incoming.GoogleTagManager)
	mergeTrackingField(&offer.TrackingIDs.GoogleAdSense, incoming.GoogleAdSense)
	mergeTrackingField(&offer.TrackingIDs.FacebookPixel, incoming.FacebookPixel)
	mergeTrackingField(&offer.TrackingIDs.Hotjar, incoming.Hotjar)
	mergeTrackingField(&offer.TrackingIDs.YandexMetrica, incoming.YandexMetrica)
	mergeTrackingField(&offer.TrackingIDs.FacebookDomainVerification, incoming.FacebookDomainVerification)
	mergeTrackingField(&offer.TrackingIDs.PinterestDomainVerify, incoming.PinterestDomainVerify)
	mergeTrackingField(&offer.TrackingIDs.YandexVerification, incoming.YandexVerification)
	mergeTrackingField(&offer.TrackingIDs.BingSiteVerification, incoming.BingSiteVerification)
	mergeTrackingField(&offer.TrackingIDs.ScriptURLs, incoming.ScriptURLs)
	mergeTrackingField(&offer.TrackingIDs.InlineScriptHashes, incoming.InlineScriptHashes)
}

func mergeTrackingField(current *[]string, incoming []string) {
	if len(incoming) == 0 {
		return
	}
	*current = uniqueStrings(append(*current, incoming...), false)
}
