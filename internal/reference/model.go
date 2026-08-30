// Package reference contains a dependency-free snapshot of the product and
// offer fields and merge semantics exercised by the production workload.
// It intentionally does not import the internal product repository.
package reference

import "time"

type Rating struct {
	Score       float64 `json:"score,omitempty"`
	Count       int64   `json:"count,omitempty"`
	Description string  `json:"description,omitempty"`
}

type ProductPrice struct {
	Currency  string         `json:"currency"`
	Price     float64        `json:"price"`
	Dollar    float64        `json:"dollar,omitempty"`
	Variables map[string]any `json:"variables,omitempty"`
	Date      *time.Time     `json:"date"`
}

type Image struct {
	URL string `json:"url"`
	Key string `json:"key"`
}

type ProductSold struct {
	Sold        int64      `json:"sold,omitempty"`
	PeriodHours int64      `json:"period_hours,omitempty"`
	RecordAt    *time.Time `json:"record_at,omitempty"`
}

type ProductStock struct {
	Stock     int64          `json:"stock,omitempty"`
	Variables map[string]any `json:"variables,omitempty"`
}

type ProductComment struct {
	Score               float64    `json:"score,omitempty"`
	Title               string     `json:"title,omitempty"`
	Description         string     `json:"description,omitempty"`
	CustomerName        string     `json:"customer_name,omitempty"`
	CustomerProfile     string     `json:"customer_profile,omitempty"`
	CustomerRatingScore float64    `json:"customer_rating_score,omitempty"`
	Location            string     `json:"location,omitempty"`
	Currency            string     `json:"currency,omitempty"`
	Price               float64    `json:"price,omitempty"`
	Date                *time.Time `json:"date,omitempty"`
}

type ProductOffer struct {
	ID               string          `json:"id"`
	UID              string          `json:"uid"`
	UIDs             []string        `json:"uids,omitempty"`
	URL              string          `json:"url"`
	Name             string          `json:"name"`
	ExpiryDetectedAt *time.Time      `json:"expiry_detected_at,omitempty"`
	ProductURL       string          `json:"product_url,omitempty"`
	ProductPrice     []*ProductPrice `json:"product_price,omitempty"`
	HasPayPal        bool            `json:"has_paypal,omitempty"`
}

type PtoClass struct {
	ClassCode        string `json:"class_code"`
	GoodsCode        string `json:"goods_code"`
	GoodsDescription string `json:"goods_description"`
}

type Product struct {
	UID                 string            `json:"uid"`
	UIDs                []string          `json:"uids,omitempty"`
	Platform            string            `json:"platform"`
	Country             string            `json:"country,omitempty"`
	ID                  string            `json:"id"`
	URL                 string            `json:"url"`
	Title               string            `json:"title,omitempty"`
	Description         string            `json:"description,omitempty"`
	Condition           string            `json:"condition,omitempty"`
	Category            []string          `json:"category,omitempty"`
	Gallery             []*Image          `json:"gallery,omitempty"`
	Brand               string            `json:"brand,omitempty"`
	Solds               []*ProductSold    `json:"solds,omitempty"`
	Prices              []*ProductPrice   `json:"prices,omitempty"`
	Stocks              []*ProductStock   `json:"stocks,omitempty"`
	CommentCount        int64             `json:"comment_count,omitempty"`
	Comments            []*ProductComment `json:"comments,omitempty"`
	Rating              *Rating           `json:"rating,omitempty"`
	Offers              []*ProductOffer   `json:"offers,omitempty"`
	AllowedCountries    []string          `json:"allowed_countries,omitempty"`
	RestrictedCountries []string          `json:"restricted_countries,omitempty"`
	SerialNumber        string            `json:"serial_number,omitempty"`
	Languages           []string          `json:"languages,omitempty"`
	CountriesFromIP     []string          `json:"countries_from_ip,omitempty"`
	SoldByPlatform      bool              `json:"sold_by_platform,omitempty"`
	FromMaybe           bool              `json:"from_maybe,omitempty"`
	Available           bool              `json:"available,omitempty"`
	FirstFoundAt        *time.Time        `json:"first_found_at,omitempty"`
	LastFoundAt         *time.Time        `json:"last_found_at,omitempty"`
	EvictedAt           *time.Time        `json:"evicted_at,omitempty"`
	Hostnames           []string          `json:"hostnames,omitempty"`
	EcommerceClass      string            `json:"ecommerce_class,omitempty"`
	PtoClass            []*PtoClass       `json:"pto_class,omitempty"`
	Brands              []string          `json:"brands,omitempty"`
	TranslatedText      string            `json:"translated_text,omitempty"`
}

type Address struct {
	Raw     string `json:"raw,omitempty"`
	Street  string `json:"street,omitempty"`
	City    string `json:"city,omitempty"`
	State   string `json:"state,omitempty"`
	Country string `json:"country,omitempty"`
	ZipCode string `json:"zip_code,omitempty"`
}

type Contact struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type TrackingIDs struct {
	GoogleAnalyticsUA          []string `json:"ga_ua,omitempty"`
	GoogleAnalyticsGA4         []string `json:"ga4,omitempty"`
	GoogleTagManager           []string `json:"gtm,omitempty"`
	GoogleAdSense              []string `json:"adsense,omitempty"`
	FacebookPixel              []string `json:"facebook_pixel,omitempty"`
	Hotjar                     []string `json:"hotjar,omitempty"`
	YandexMetrica              []string `json:"yandex_metrica,omitempty"`
	FacebookDomainVerification []string `json:"facebook_domain_verification,omitempty"`
	PinterestDomainVerify      []string `json:"pinterest_domain_verify,omitempty"`
	YandexVerification         []string `json:"yandex_verification,omitempty"`
	BingSiteVerification       []string `json:"bing_site_verification,omitempty"`
	ScriptURLs                 []string `json:"script_urls,omitempty"`
	InlineScriptHashes         []string `json:"inline_script_hashes,omitempty"`
}

type Offer struct {
	UID           string       `json:"uid"`
	UIDs          []string     `json:"uids,omitempty"`
	Platform      string       `json:"platform"`
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	URL           string       `json:"url"`
	Country       string       `json:"country,omitempty"`
	BusinessName  string       `json:"business_name,omitempty"`
	Addresses     []*Address   `json:"addresses,omitempty"`
	Contacts      []*Contact   `json:"contacts,omitempty"`
	Bio           string       `json:"bio,omitempty"`
	MemberSince   *time.Time   `json:"member_since,omitempty"`
	Sold          int64        `json:"sold,omitempty"`
	CommentCount  int64        `json:"comment_count,omitempty"`
	FollowerCount int64        `json:"follower_count,omitempty"`
	ProductCount  int64        `json:"product_count,omitempty"`
	FromMaybe     bool         `json:"from_maybe,omitempty"`
	FirstFoundAt  *time.Time   `json:"first_found_at,omitempty"`
	LastFoundAt   *time.Time   `json:"last_found_at,omitempty"`
	EvictedAt     *time.Time   `json:"evicted_at,omitempty"`
	Hostnames     []string     `json:"hostnames,omitempty"`
	TrackingIDs   *TrackingIDs `json:"tracking_ids,omitempty"`
	HasPayPal     bool         `json:"has_paypal,omitempty"`
}
