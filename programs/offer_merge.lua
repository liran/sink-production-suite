local array = sink.v1.array
local object = sink.v1.object

local function merge_unique(current, incoming, field)
    local additions = incoming[field]
    if additions == nil or #additions == 0 then
        return
    end
    local values = current[field] or json.array()
    array.append_all(values, additions)
    current[field] = array.deduplicate(values, function(item)
        return item
    end)
end

local scalar_fields = {
    "uid",
    "platform",
    "id",
    "name",
    "url",
    "country",
    "business_name",
    "bio",
}

local tracking_fields = {
    "ga_ua",
    "ga4",
    "gtm",
    "adsense",
    "facebook_pixel",
    "hotjar",
    "yandex_metrica",
    "facebook_domain_verification",
    "pinterest_domain_verify",
    "yandex_verification",
    "bing_site_verification",
    "script_urls",
    "inline_script_hashes",
}

local function merge_one(current, incoming)
    if current == nil then
        current = json.object()
    end

    for i = 1, #scalar_fields do
        object.replace_nonempty_string(current, incoming, scalar_fields[i])
    end
    merge_unique(current, incoming, "uids")

    if incoming.addresses ~= nil and #incoming.addresses > 0 then
        local addresses = current.addresses or json.array()
        array.append_all(addresses, incoming.addresses)
        current.addresses = array.deduplicate(addresses, function(item)
            return (item.raw or "") .. (item.country or "")
        end)
    end
    if current.addresses ~= nil and #current.addresses > 10 then
        current.addresses = array.keep_tail(current.addresses, 10)
    end

    if incoming.contacts ~= nil and #incoming.contacts > 0 then
        local contacts = current.contacts or json.array()
        array.append_all(contacts, incoming.contacts)
        current.contacts = array.deduplicate(contacts, function(item)
            return (item.type or "") .. (item.value or "")
        end)
    end

    if incoming.member_since ~= nil then
        current.member_since = incoming.member_since
    end
    if incoming.sold ~= nil and incoming.sold ~= 0 then
        current.sold = incoming.sold
    end
    if incoming.follower_count ~= nil and incoming.follower_count ~= 0 then
        current.follower_count = incoming.follower_count
    end
    if incoming.product_count ~= nil and incoming.product_count ~= 0 then
        current.product_count = incoming.product_count
    end

    current.last_found_at = sink.v1.time.now()
    if current.first_found_at == nil then
        current.first_found_at = incoming.first_found_at or sink.v1.time.now()
    end
    if incoming.evicted_at ~= nil then
        current.evicted_at = incoming.evicted_at
    end
    object.replace_nonempty_array(current, incoming, "hostnames")

    if incoming.tracking_ids ~= nil then
        current.tracking_ids = current.tracking_ids or json.object()
        for i = 1, #tracking_fields do
            merge_unique(current.tracking_ids, incoming.tracking_ids, tracking_fields[i])
        end
    end

    current.has_paypal = incoming.has_paypal == true
    return current
end

return function(current, incoming)
    local result = json.object()
    if current ~= nil then
        result = merge_one(result, current)
    end
    result = merge_one(result, incoming)
    result.evicted_at = nil
    return result
end
