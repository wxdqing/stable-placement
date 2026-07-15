
local e=expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"set","nodes") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"zset","index") or expect_type(KEYS[5],"stream","events");if e then return e end
local raw=redis.call("GET",KEYS[1]);if not raw then return "node_not_found" end;local node,de=decode(raw,"node");if de then return de end
if node["NodeKey"]~=ARGV[2] then return redis.error_reply("IDENTITY_MISMATCH") end
if node["NodeSessionID"]~=ARGV[1] then return "invalid_node_session" end;if ARGV[3]=="true" and redis.call("ZCARD",KEYS[4])>0 then return "node_has_placements" end
local score=redis.call("ZSCORE",KEYS[3],ARGV[2]);if node["Status"]=="offline" then if score and tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end elseif node["Status"]=="active" or node["Status"]=="draining" then if not score or tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end else return redis.error_reply("INVALID_NODE_STATUS") end
redis.call("DEL",KEYS[1]);redis.call("SREM",KEYS[2],ARGV[2]);redis.call("ZREM",KEYS[3],ARGV[2]);node_event(KEYS[5],ARGV[4],node["NodeIdentity"],node["NodeSessionID"],node["NodeType"],node["NodeGroup"],node["NodeName"],tostring(node["Lease"]["Version"]));return "ok"

