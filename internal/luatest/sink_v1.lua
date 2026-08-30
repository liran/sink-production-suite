local function require_array(value, name)
    if type(value) ~= "table" then
        error(name .. " must be a JSON array")
    end
end

local array = {}

function array.append_all(target, source)
    require_array(target, "target")
    if source == nil then
        return target
    end
    require_array(source, "source")
    for index = 1, #source do
        target[#target + 1] = source[index]
    end
    return target
end

function array.deduplicate(items, key_function)
    require_array(items, "items")
    if type(key_function) ~= "function" then
        error("key_function must be a function")
    end
    local result = json.array()
    local seen = {}
    for index = 1, #items do
        local item = items[index]
        local key = key_function(item)
        local key_type = type(key)
        if key_type ~= "string" and key_type ~= "number" and key_type ~= "boolean" then
            error("key_function must return a string, number, or boolean")
        end
        if seen[key] == nil then
            seen[key] = true
            result[#result + 1] = item
        end
    end
    return result
end

function array.keep_tail(items, limit)
    require_array(items, "items")
    if type(limit) ~= "number" or limit < 0 or limit % 1 ~= 0 then
        error("limit must be a non-negative integer")
    end
    local result = json.array()
    local first = math.max(1, #items - limit + 1)
    if limit == 0 then
        return result
    end
    for index = first, #items do
        result[#result + 1] = items[index]
    end
    return result
end

function array.union_strings(left, right)
    local result = json.array()
    local seen = {}
    local function append(source)
        if source == nil then
            return
        end
        require_array(source, "source")
        for index = 1, #source do
            local value = source[index]
            if type(value) ~= "string" then
                error("array item must be a string")
            end
            if seen[value] == nil then
                seen[value] = true
                result[#result + 1] = value
            end
        end
    end
    append(left)
    append(right)
    return result
end

local object = {}

function object.replace_nonempty_string(target, source, field)
    local value = source[field]
    if value == nil or value == "" then
        return
    end
    if type(value) ~= "string" then
        error("source field must be a string or nil")
    end
    target[field] = value
end

function object.replace_nonempty_array(target, source, field)
    local value = source[field]
    if value == nil then
        return
    end
    require_array(value, "source field")
    if #value > 0 then
        target[field] = value
    end
end

local v1 = {
    array = array,
    object = object,
    time = {},
}
sink = {v1 = v1}
