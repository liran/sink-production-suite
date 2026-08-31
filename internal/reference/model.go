// Package reference contains a dependency-free snapshot of the product and
// offer fields and merge semantics exercised by the production workload.
// It intentionally does not import the internal product repository.
package reference

import "time"

type Rating struct {
	Score       float64 `json:"score,omitempty" bson:"score,omitempty"`
	Count       int64   `json:"count,omitempty" bson:"count,omitempty"`
	Description string  `json:"description,omitempty" bson:"description,omitempty"`
}

type ProductPrice struct {
	Currency  string         `json:"currency" bson:"currency"`
	Price     float64        `json:"price" bson:"price"`
	Dollar    float64        `json:"dollar,omitempty" bson:"dollar,omitempty"`
	Variables map[string]any `json:"variables,omitempty" bson:"variables,omitempty"`
	Date      *time.Time     `json:"date" bson:"date"`
}

type Image struct {
	URL string `json:"url" bson:"url"`
	Key string `json:"key" bson:"key"`
}

type ProductSold struct {
	Sold        int64      `json:"sold,omitempty" bson:"sold,omitempty"`
	PeriodHours int64      `json:"period_hours,omitempty" bson:"period_hours,omitempty"`
	RecordAt    *time.Time `json:"record_at,omitempty" bson:"record_at,omitempty"`
}

type ProductStock struct {
	Stock     int64          `json:"stock,omitempty" bson:"stock,omitempty"`
	Variables map[string]any `json:"variables,omitempty" bson:"variables,omitempty"`
}

type ProductComment struct {
	Score               float64    `json:"score,omitempty" bson:"score,omitempty"`
	Title               string     `json:"title,omitempty" bson:"title,omitempty"`
	Description         string     `json:"description,omitempty" bson:"description,omitempty"`
	CustomerName        string     `json:"customer_name,omitempty" bson:"customer_name,omitempty"`
	CustomerProfile     string     `json:"customer_profile,omitempty" bson:"customer_profile,omitempty"`
	CustomerRatingScore float64    `json:"customer_rating_score,omitempty" bson:"customer_rating_score,omitempty"`
	Location            string     `json:"location,omitempty" bson:"location,omitempty"`
	Currency            string     `json:"currency,omitempty" bson:"currency,omitempty"`
	Price               float64    `json:"price,omitempty" bson:"price,omitempty"`
	Date                *time.Time `json:"date,omitempty" bson:"date,omitempty"`
}

type ProductOffer struct {
	ID               string          `json:"id" bson:"id"`
	UID              string          `json:"uid" bson:"uid"`
	UIDs             []string        `json:"uids,omitempty" bson:"uids,omitempty"`
	URL              string          `json:"url" bson:"url"`
	Name             string          `json:"name" bson:"name"`
	ExpiryDetectedAt *time.Time      `json:"expiry_detected_at,omitempty" bson:"expiry_detected_at,omitempty"`
	ProductURL       string          `json:"product_url,omitempty" bson:"product_url,omitempty"`
	ProductPrice     []*ProductPrice `json:"product_price,omitempty" bson:"product_price,omitempty"`
	HasPayPal        bool            `json:"has_paypal,omitempty" bson:"has_paypal,omitempty"`
}

type PtoClass struct {
	ClassCode        string `json:"class_code" bson:"class_code"`
	GoodsCode        string `json:"goods_code" bson:"goods_code"`
	GoodsDescription string `json:"goods_description" bson:"goods_description"`
}

type Product struct {
	UID                 string            `json:"uid" bson:"_id"`
	UIDs                []string          `json:"uids,omitempty" bson:"uids,omitempty"`
	Platform            string            `json:"platform" bson:"platform"`
	Country             string            `json:"country,omitempty" bson:"country,omitempty"`
	ID                  string            `json:"id" bson:"id"`
	URL                 string            `json:"url" bson:"url"`
	Title               string            `json:"title,omitempty" bson:"title,omitempty"`
	Description         string            `json:"description,omitempty" bson:"description,omitempty"`
	Condition           string            `json:"condition,omitempty" bson:"condition,omitempty"`
	Category            []string          `json:"category,omitempty" bson:"category,omitempty"`
	Gallery             []*Image          `json:"gallery,omitempty" bson:"gallery,omitempty"`
	Brand               string            `json:"brand,omitempty" bson:"brand,omitempty"`
	Solds               []*ProductSold    `json:"solds,omitempty" bson:"solds,omitempty"`
	Prices              []*ProductPrice   `json:"prices,omitempty" bson:"prices,omitempty"`
	Stocks              []*ProductStock   `json:"stocks,omitempty" bson:"stocks,omitempty"`
	CommentCount        int64             `json:"comment_count,omitempty" bson:"comment_count,omitempty"`
	Comments            []*ProductComment `json:"comments,omitempty" bson:"comments,omitempty"`
	Rating              *Rating           `json:"rating,omitempty" bson:"rating,omitempty"`
	Offers              []*ProductOffer   `json:"offers,omitempty" bson:"offers,omitempty"`
	AllowedCountries    []string          `json:"allowed_countries,omitempty" bson:"allowed_countries,omitempty"`
	RestrictedCountries []string          `json:"restricted_countries,omitempty" bson:"restricted_countries,omitempty"`
	SerialNumber        string            `json:"serial_number,omitempty" bson:"serial_number,omitempty"`
	Languages           []string          `json:"languages,omitempty" bson:"languages,omitempty"`
	CountriesFromIP     []string          `json:"countries_from_ip,omitempty" bson:"countries_from_ip,omitempty"`
	SoldByPlatform      bool              `json:"sold_by_platform,omitempty" bson:"sold_by_platform,omitempty"`
	FromMaybe           bool              `json:"from_maybe,omitempty" bson:"from_maybe,omitempty"`
	Available           bool              `json:"available,omitempty" bson:"available,omitempty"`
	FirstFoundAt        *time.Time        `json:"first_found_at,omitempty" bson:"first_found_at,omitempty"`
	LastFoundAt         *time.Time        `json:"last_found_at,omitempty" bson:"last_found_at,omitempty"`
	EvictedAt           *time.Time        `json:"evicted_at,omitempty" bson:"evicted_at,omitempty"`
	Hostnames           []string          `json:"hostnames,omitempty" bson:"hostnames,omitempty"`
	EcommerceClass      string            `json:"ecommerce_class,omitempty" bson:"ecommerce_class,omitempty"`
	PtoClass            []*PtoClass       `json:"pto_class,omitempty" bson:"pto_class,omitempty"`
	Brands              []string          `json:"brands,omitempty" bson:"brands,omitempty"`
	TranslatedText      string            `json:"translated_text,omitempty" bson:"translated_text,omitempty"`
}

type Address struct {
	Raw     string `json:"raw,omitempty" bson:"raw,omitempty"`
	Street  string `json:"street,omitempty" bson:"street,omitempty"`
	City    string `json:"city,omitempty" bson:"city,omitempty"`
	State   string `json:"state,omitempty" bson:"state,omitempty"`
	Country string `json:"country,omitempty" bson:"country,omitempty"`
	ZipCode string `json:"zip_code,omitempty" bson:"zip_code,omitempty"`
}

type Contact struct {
	Type  string `json:"type" bson:"type"`
	Value string `json:"value" bson:"value"`
}

type TrackingIDs struct {
	GoogleAnalyticsUA          []string `json:"ga_ua,omitempty" bson:"ga_ua,omitempty"`
	GoogleAnalyticsGA4         []string `json:"ga4,omitempty" bson:"ga4,omitempty"`
	GoogleTagManager           []string `json:"gtm,omitempty" bson:"gtm,omitempty"`
	GoogleAdSense              []string `json:"adsense,omitempty" bson:"adsense,omitempty"`
	FacebookPixel              []string `json:"facebook_pixel,omitempty" bson:"facebook_pixel,omitempty"`
	Hotjar                     []string `json:"hotjar,omitempty" bson:"hotjar,omitempty"`
	YandexMetrica              []string `json:"yandex_metrica,omitempty" bson:"yandex_metrica,omitempty"`
	FacebookDomainVerification []string `json:"facebook_domain_verification,omitempty" bson:"facebook_domain_verification,omitempty"`
	PinterestDomainVerify      []string `json:"pinterest_domain_verify,omitempty" bson:"pinterest_domain_verify,omitempty"`
	YandexVerification         []string `json:"yandex_verification,omitempty" bson:"yandex_verification,omitempty"`
	BingSiteVerification       []string `json:"bing_site_verification,omitempty" bson:"bing_site_verification,omitempty"`
	ScriptURLs                 []string `json:"script_urls,omitempty" bson:"script_urls,omitempty"`
	InlineScriptHashes         []string `json:"inline_script_hashes,omitempty" bson:"inline_script_hashes,omitempty"`
}

type Offer struct {
	UID           string       `json:"uid" bson:"_id"`
	UIDs          []string     `json:"uids,omitempty" bson:"uids,omitempty"`
	Platform      string       `json:"platform" bson:"platform"`
	ID            string       `json:"id" bson:"id"`
	Name          string       `json:"name" bson:"name"`
	URL           string       `json:"url" bson:"url"`
	Country       string       `json:"country,omitempty" bson:"country,omitempty"`
	BusinessName  string       `json:"business_name,omitempty" bson:"business_name,omitempty"`
	Addresses     []*Address   `json:"addresses,omitempty" bson:"addresses,omitempty"`
	Contacts      []*Contact   `json:"contacts,omitempty" bson:"contacts,omitempty"`
	Bio           string       `json:"bio,omitempty" bson:"bio,omitempty"`
	MemberSince   *time.Time   `json:"member_since,omitempty" bson:"member_since,omitempty"`
	Sold          int64        `json:"sold,omitempty" bson:"sold,omitempty"`
	CommentCount  int64        `json:"comment_count,omitempty" bson:"comment_count,omitempty"`
	FollowerCount int64        `json:"follower_count,omitempty" bson:"follower_count,omitempty"`
	ProductCount  int64        `json:"product_count,omitempty" bson:"product_count,omitempty"`
	FromMaybe     bool         `json:"from_maybe,omitempty" bson:"from_maybe,omitempty"`
	FirstFoundAt  *time.Time   `json:"first_found_at,omitempty" bson:"first_found_at,omitempty"`
	LastFoundAt   *time.Time   `json:"last_found_at,omitempty" bson:"last_found_at,omitempty"`
	EvictedAt     *time.Time   `json:"evicted_at,omitempty" bson:"evicted_at,omitempty"`
	Hostnames     []string     `json:"hostnames,omitempty" bson:"hostnames,omitempty"`
	TrackingIDs   *TrackingIDs `json:"tracking_ids,omitempty" bson:"tracking_ids,omitempty"`
	HasPayPal     bool         `json:"has_paypal,omitempty" bson:"has_paypal,omitempty"`
}
