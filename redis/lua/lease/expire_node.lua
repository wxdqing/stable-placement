
local e=expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"zset","leases") or expect_type(KEYS[3],"stream","events");if e then return e end
local current_score=redis.call("ZSCORE",KEYS[2],ARGV[1]);if not current_score then return "stale" end
if tostring(current_score)~=tostring(ARGV[3]) and tonumber(current_score)~=tonumber(ARGV[3]) then return "stale" end
local raw=redis.call("GET",KEYS[1]);if not raw then redis.call("ZREM",KEYS[2],ARGV[1]);return "stale" end
if raw~=ARGV[2] then return "stale" end
local node,de=decode(raw,"node");if de then return de end
if tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1)~=tonumber(current_score) or tostring(node["Lease"]["Version"])~=ARGV[4] or node["NodeSessionID"]~=ARGV[5] then return "stale" end
if node["NodeKey"]~=ARGV[1] or tonumber(node["Lease"]["Version"] or 0)<=0 or tonumber(node["Lease"]["TTLMillis"] or 0)<=0 then return redis.error_reply("INVALID_NODE") end
if node["Status"]=="offline" then redis.call("ZREM",KEYS[2],ARGV[1]);return "stale" end
if node["Status"]~="active" and node["Status"]~="draining" then return redis.error_reply("INVALID_NODE_STATUS") end
if tonumber(current_score)>now_millis() then return "not_due" end
node["Status"]="offline";redis.call("SET",KEYS[1],cjson.encode(node));redis.call("ZREM",KEYS[2],ARGV[1]);node_event(KEYS[3],ARGV[6],node["NodeIdentity"],node["NodeSessionID"],node["NodeType"],node["NodeGroup"],node["NodeName"],tostring(node["Lease"]["Version"]));return "expired"

