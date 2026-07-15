
local e=expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"zset","leases");if e then return e end
local raw=redis.call("GET",KEYS[1]);if not raw then return "node_not_found" end
local node,de=decode(raw,"node");if de then return de end
if node["NodeKey"]~=ARGV[2] or tonumber(node["Lease"]["Version"] or 0)<=0 or tonumber(node["Lease"]["TTLMillis"] or 0)<=0 then return redis.error_reply("INVALID_NODE") end
if node["NodeSessionID"]~=ARGV[1] then return "invalid_node_session" end
local score=redis.call("ZSCORE",KEYS[2],ARGV[2])
if node["Status"]=="offline" then if score and tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end;return "node_not_found" end
if node["Status"]~="active" and node["Status"]~="draining" then return redis.error_reply("INVALID_NODE_STATUS") end
if not score or tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end
local now=now_millis();if tonumber(node["Lease"]["ExpireAtUnixMilli"] or 0)<=now then return "node_lease_expired" end
if ARGV[3]~="" then local metrics,me=decode(ARGV[3],"metrics");if me then return me end;if not valid_metric_number(metrics["CPUAvailableMilliCores"]) or not valid_metric_number(metrics["MemoryAvailableBytes"]) or not valid_metric_number(metrics["Goroutines"]) then return "invalid_metrics" end;metrics["UpdatedAtUnixMilli"]=now;node["Metrics"]=metrics end
node["Lease"]["Version"]=tonumber(node["Lease"]["Version"])+1
local expiry=now+tonumber(node["Lease"]["TTLMillis"]);if expiry<tonumber(score) then expiry=tonumber(score) end
node["Lease"]["ExpireAtUnixMilli"]=expiry
redis.call("SET",KEYS[1],cjson.encode(node));redis.call("ZADD",KEYS[2],expiry,ARGV[2]);return {"ok",tostring(node["Lease"]["Version"]),tostring(expiry-now)}

