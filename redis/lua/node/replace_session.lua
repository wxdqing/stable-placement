
local e=expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"set","nodes") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"stream","events");if e then return e end
local incoming,de=decode(ARGV[1],"incoming_node");if de then return de end
local oldraw=redis.call("GET",KEYS[1]);local old,oe=decode(oldraw,"node");if oe then return oe end
if old and (old["NodeIdentity"]~=incoming["NodeIdentity"] or old["NodeType"]~=incoming["NodeType"] or old["NodeGroup"]~=incoming["NodeGroup"] or old["NodeName"]~=incoming["NodeName"] or old["NodeKey"]~=ARGV[3]) then return redis.error_reply("IDENTITY_MISMATCH") end
if old and old["NodeSessionID"]==incoming["NodeSessionID"] then return "invalid_node_session" end
if old then local score=redis.call("ZSCORE",KEYS[3],ARGV[3]);if old["Status"]=="offline" then if score and tonumber(score)~=tonumber(old["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end elseif old["Status"]=="active" or old["Status"]=="draining" then if not score or tonumber(score)~=tonumber(old["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end else return redis.error_reply("INVALID_NODE_STATUS") end end
local now=now_millis();incoming["Status"]="active";incoming["Lease"]={Version=INITIAL_NODE_LEASE_VERSION,TTLMillis=tonumber(ARGV[2]),ExpireAtUnixMilli=now+tonumber(ARGV[2])}
redis.call("SET",KEYS[1],cjson.encode(incoming));redis.call("SADD",KEYS[2],ARGV[3]);redis.call("ZADD",KEYS[3],incoming["Lease"]["ExpireAtUnixMilli"],ARGV[3]);node_event(KEYS[4],ARGV[4],incoming["NodeIdentity"],incoming["NodeSessionID"],incoming["NodeType"],incoming["NodeGroup"],incoming["NodeName"],tostring(INITIAL_NODE_LEASE_VERSION))
return {"ok",oldraw or "",tostring(INITIAL_NODE_LEASE_VERSION),tostring(tonumber(ARGV[2]))}
