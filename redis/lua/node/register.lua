
local e = expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"set","nodes") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"stream","events")
if e then return e end
local incoming, de = decode(ARGV[1],"incoming_node"); if de then return de end
local raw = redis.call("GET",KEYS[1]); local existing, olde = decode(raw,"node"); if olde then return olde end
local now = now_millis()
	if existing then
	  if existing["NodeKey"]~=ARGV[3] or existing["NodeIdentity"]~=incoming["NodeIdentity"] or existing["NodeType"]~=incoming["NodeType"] or existing["NodeGroup"]~=incoming["NodeGroup"] or existing["NodeName"]~=incoming["NodeName"] then return redis.error_reply("IDENTITY_MISMATCH") end
	  if existing["NodeSessionID"] ~= incoming["NodeSessionID"] then return "invalid_node_session" end
	  local score=redis.call("ZSCORE",KEYS[3],ARGV[3])
	  if existing["Status"] == "offline" then if score and tonumber(score)~=tonumber(existing["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end;return "node_lease_expired" end
	  if existing["Status"] ~= "active" and existing["Status"] ~= "draining" then return redis.error_reply("INVALID_NODE_STATUS") end
	  if not score or tonumber(score)~=tonumber(existing["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end
	  if tonumber(existing["Lease"]["ExpireAtUnixMilli"] or 0) <= now then return "node_lease_expired" end
	  return {"ok",tostring(existing["Lease"]["Version"]),tostring(tonumber(existing["Lease"]["ExpireAtUnixMilli"])-now)}
	end
incoming["Status"]="active"; incoming["Lease"]={Version=1,TTLMillis=tonumber(ARGV[2]),ExpireAtUnixMilli=now+tonumber(ARGV[2])}
local encoded=cjson.encode(incoming)
redis.call("SET",KEYS[1],encoded);redis.call("SADD",KEYS[2],ARGV[3]);redis.call("ZADD",KEYS[3],incoming["Lease"]["ExpireAtUnixMilli"],ARGV[3])
node_event(KEYS[4],ARGV[4],incoming["NodeIdentity"],incoming["NodeSessionID"],incoming["NodeType"],incoming["NodeGroup"],incoming["NodeName"],"1")
return {"ok","1",tostring(tonumber(ARGV[2]))}

