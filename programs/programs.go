// Package programs exposes the versioned Lua merge programs qualified by this suite.
package programs

import _ "embed"

//go:embed product_merge.lua
var ProductMerge []byte

//go:embed offer_merge.lua
var OfferMerge []byte
