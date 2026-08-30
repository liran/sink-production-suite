local array = sink.v1.array
local object = sink.v1.object

local function canonical(value)
    local value_type = type(value)
    if value_type == "nil" then
        return "null"
    end
    if value_type == "boolean" or value_type == "number" then
        return tostring(value)
    end
    if value_type == "string" then
        return string.format("%q", value)
    end
    if value_type ~= "table" then
        error("unsupported value type: " .. value_type)
    end

    local keys = {}
    for key in pairs(value) do
        keys[#keys + 1] = key
    end
    table.sort(keys, function(left, right)
        return tostring(left) < tostring(right)
    end)

    local parts = {"{"}
    for i = 1, #keys do
        local key = keys[i]
        parts[#parts + 1] = canonical(key)
        parts[#parts + 1] = ":"
        parts[#parts + 1] = canonical(value[key])
        parts[#parts + 1] = ","
    end
    parts[#parts + 1] = "}"
    return table.concat(parts)
end

local function merge_string_set(current, incoming, field)
    local values = incoming[field]
    if values == nil or #values == 0 then
        return
    end
    current[field] = array.union_strings(current[field], values)
end

local function history(current, incoming, field, key_function, limit)
    local items = current[field]
    local additions = incoming[field]
    if additions ~= nil and #additions > 0 then
        items = items or json.array()
        array.append_all(items, additions)
        items = array.deduplicate(items, key_function)
        current[field] = items
    end
    if items ~= nil and #items > limit then
        current[field] = array.keep_tail(items, limit)
    end
end

local scalar_fields = {
    "platform",
    "country",
    "id",
    "url",
    "title",
    "description",
    "condition",
    "ecommerce_class",
    "translated_text",
    "serial_number",
}

local array_replace_fields = {
    "category",
    "gallery",
    "prices",
    "hostnames",
    "pto_class",
    "brands",
}

local function merge_one(current, incoming)
    if current == nil then
        current = json.object()
    end

    local uids = current.uids or json.array()
    if current.uid ~= nil and current.uid ~= "" then
        uids[#uids + 1] = current.uid
    end
    if incoming.uid ~= nil and incoming.uid ~= "" then
        uids[#uids + 1] = incoming.uid
        current.uid = incoming.uid
    end
    array.append_all(uids, incoming.uids)
    uids = array.deduplicate(uids, function(item)
        return item
    end)
    local nonempty_uids = json.array()
    for i = 1, #uids do
        if uids[i] ~= "" then
            nonempty_uids[#nonempty_uids + 1] = uids[i]
        end
    end
    current.uids = nonempty_uids

    for i = 1, #scalar_fields do
        object.replace_nonempty_string(current, incoming, scalar_fields[i])
    end
    for i = 1, #array_replace_fields do
        object.replace_nonempty_array(current, incoming, array_replace_fields[i])
    end

    if incoming.brand ~= nil and incoming.brand ~= "" then
        current.brand = utf8.upper(incoming.brand)
    end

    history(current, incoming, "solds", function(item)
        local record_at = item.record_at or ""
        if #record_at > 10 then
            record_at = string.sub(record_at, 1, 10)
        end
        return tostring(item.sold or 0) .. "-" .. tostring(item.period_hours or 0) .. "-" .. record_at
    end, 20)

    history(current, incoming, "stocks", function(item)
        return tostring(item.stock or 0) .. canonical(item.variables or {})
    end, 20)

    if incoming.comment_count ~= nil and incoming.comment_count ~= 0 then
        current.comment_count = incoming.comment_count
    end
    history(current, incoming, "comments", canonical, 20)

    if incoming.rating ~= nil then
        current.rating = incoming.rating
    end

    if incoming.offers ~= nil and #incoming.offers > 0 then
        local offers = json.array()
        array.append_all(offers, incoming.offers)
        array.append_all(offers, current.offers)
        current.offers = array.deduplicate(offers, function(item)
            return item.uid or ""
        end)
    end
    if current.offers ~= nil and #current.offers > 50 then
        current.offers = array.keep_tail(current.offers, 50)
    end

    merge_string_set(current, incoming, "allowed_countries")
    merge_string_set(current, incoming, "restricted_countries")
    merge_string_set(current, incoming, "languages")
    merge_string_set(current, incoming, "countries_from_ip")

    current.last_found_at = sink.v1.time.now()
    if current.first_found_at == nil then
        current.first_found_at = incoming.first_found_at or sink.v1.time.now()
    end
    if incoming.evicted_at ~= nil then
        current.evicted_at = incoming.evicted_at
    end

    current.sold_by_platform = incoming.sold_by_platform == true
    current.available = incoming.available == true
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
