local function expect_type(key, expected, label)
  local actual = redis.call("TYPE", key)
  if type(actual) == "table" then actual = actual["ok"] end
  if actual ~= "none" and actual ~= expected then return redis.error_reply("WRONGTYPE " .. label) end
  return nil
end

local function decode(raw, label)
  if not raw then return nil, nil end
  local ok, value = pcall(cjson.decode, raw)
  if not ok or type(value) ~= "table" then return nil, redis.error_reply("INVALID_JSON " .. label) end
  return value, nil
end

local function now_millis()
  local t = redis.call("TIME")
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function read_counter(key, label, maximum)
  local raw=redis.call("GET",key)
  if not raw then return "0",nil end
  if not string.match(raw,"^%d+$") or (#raw>1 and string.sub(raw,1,1)=="0") or #raw>#maximum or (#raw==#maximum and raw>=maximum) then return nil,redis.error_reply("INVALID_COUNTER "..label) end
  return raw,nil
end
