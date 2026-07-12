package redis

const luaHelpers = `
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
local function decimal_mod(value,divisor)
  local remainder=0
  for index=1,#value do remainder=(remainder*10+tonumber(string.sub(value,index,index)))%divisor end
  return remainder
end
local function event(stream, kind, grain, node, session, placement_version, lease_version)
  redis.call("XADD", stream, "*", "type", kind, "grain_key", grain or "", "node_identity", node or "", "node_session_id", session or "", "node_type", "", "node_group", "", "node_name", "", "placement_version", placement_version or "0", "node_lease_version", lease_version or "0")
end
local function node_event(stream, kind, node, session, node_type, node_group, node_name, lease_version)
  redis.call("XADD", stream, "*", "type", kind, "grain_key", "", "node_identity", node or "", "node_session_id", session or "", "node_type", node_type or "", "node_group", node_group or "", "node_name", node_name or "", "placement_version", "0", "node_lease_version", lease_version or "0")
end
`

const registerNodeLua = luaHelpers + `
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
`

const renewNodeLua = luaHelpers + `
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
node["Lease"]["Version"]=tonumber(node["Lease"]["Version"])+1
local expiry=now+tonumber(node["Lease"]["TTLMillis"]);if expiry<tonumber(score) then expiry=tonumber(score) end
node["Lease"]["ExpireAtUnixMilli"]=expiry
redis.call("SET",KEYS[1],cjson.encode(node));redis.call("ZADD",KEYS[2],expiry,ARGV[2]);return {"ok",tostring(node["Lease"]["Version"]),tostring(expiry-now)}
`

const replaceNodeSessionLua = luaHelpers + `
local e=expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"set","nodes") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"stream","events");if e then return e end
local incoming,de=decode(ARGV[1],"incoming_node");if de then return de end
local oldraw=redis.call("GET",KEYS[1]);local old,oe=decode(oldraw,"node");if oe then return oe end
if old and (old["NodeIdentity"]~=incoming["NodeIdentity"] or old["NodeType"]~=incoming["NodeType"] or old["NodeGroup"]~=incoming["NodeGroup"] or old["NodeName"]~=incoming["NodeName"] or old["NodeKey"]~=ARGV[3]) then return redis.error_reply("IDENTITY_MISMATCH") end
if old and old["NodeSessionID"]==incoming["NodeSessionID"] then return "invalid_node_session" end
if old then local score=redis.call("ZSCORE",KEYS[3],ARGV[3]);if old["Status"]=="offline" then if score and tonumber(score)~=tonumber(old["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end elseif old["Status"]=="active" or old["Status"]=="draining" then if not score or tonumber(score)~=tonumber(old["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end else return redis.error_reply("INVALID_NODE_STATUS") end end
local now=now_millis();incoming["Status"]="active";incoming["Lease"]={Version=1,TTLMillis=tonumber(ARGV[2]),ExpireAtUnixMilli=now+tonumber(ARGV[2])}
redis.call("SET",KEYS[1],cjson.encode(incoming));redis.call("SADD",KEYS[2],ARGV[3]);redis.call("ZADD",KEYS[3],incoming["Lease"]["ExpireAtUnixMilli"],ARGV[3]);node_event(KEYS[4],ARGV[4],incoming["NodeIdentity"],incoming["NodeSessionID"],incoming["NodeType"],incoming["NodeGroup"],incoming["NodeName"],"1")
return {"ok",oldraw or "","1",tostring(tonumber(ARGV[2]))}
`

const expireNodeLeaseLua = luaHelpers + `
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
`

const unregisterNodeLua = luaHelpers + `
local e=expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"set","nodes") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"zset","index") or expect_type(KEYS[5],"stream","events");if e then return e end
local raw=redis.call("GET",KEYS[1]);if not raw then return "node_not_found" end;local node,de=decode(raw,"node");if de then return de end
if node["NodeKey"]~=ARGV[2] then return redis.error_reply("IDENTITY_MISMATCH") end
if node["NodeSessionID"]~=ARGV[1] then return "invalid_node_session" end;if ARGV[3]=="true" and redis.call("ZCARD",KEYS[4])>0 then return "node_has_placements" end
local score=redis.call("ZSCORE",KEYS[3],ARGV[2]);if node["Status"]=="offline" then if score and tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end elseif node["Status"]=="active" or node["Status"]=="draining" then if not score or tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end else return redis.error_reply("INVALID_NODE_STATUS") end
redis.call("DEL",KEYS[1]);redis.call("SREM",KEYS[2],ARGV[2]);redis.call("ZREM",KEYS[3],ARGV[2]);node_event(KEYS[5],ARGV[4],node["NodeIdentity"],node["NodeSessionID"],node["NodeType"],node["NodeGroup"],node["NodeName"],tostring(node["Lease"]["Version"]));return "ok"
`

const drainNodeLua = luaHelpers + `
local e=expect_type(KEYS[1],"string","node") or expect_type(KEYS[2],"set","invalid") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"stream","events");if e then return e end
local raw=redis.call("GET",KEYS[1]);if not raw then return "node_not_found" end;local node,de=decode(raw,"node");if de then return de end;if redis.call("SISMEMBER",KEYS[2],ARGV[1])==0 then return "node_not_invalid" end
if node["NodeKey"]~=ARGV[3] then return redis.error_reply("IDENTITY_MISMATCH") end;local score=redis.call("ZSCORE",KEYS[3],ARGV[3]);if node["Status"]=="offline" then if score and tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end;return "node_not_found" end;if node["Status"]~="active" and node["Status"]~="draining" then return redis.error_reply("INVALID_NODE_STATUS") end;if not score or tonumber(score)~=tonumber(node["Lease"]["ExpireAtUnixMilli"] or -1) then return redis.error_reply("LEASE_SCORE_MISMATCH") end
node["Status"]="draining";redis.call("SET",KEYS[1],cjson.encode(node));node_event(KEYS[4],ARGV[2],node["NodeIdentity"],node["NodeSessionID"],node["NodeType"],node["NodeGroup"],node["NodeName"],tostring(node["Lease"]["Version"]));return "ok"
`
const markInvalidLua = luaHelpers + `local e=expect_type(KEYS[1],"set","invalid") or expect_type(KEYS[2],"stream","events");if e then return e end;redis.call("SADD",KEYS[1],ARGV[1]);node_event(KEYS[2],ARGV[5],ARGV[2],"",ARGV[3],ARGV[4],ARGV[1],"0");return "ok"`
const restoreNodeLua = luaHelpers + `local e=expect_type(KEYS[1],"set","invalid") or expect_type(KEYS[2],"stream","events");if e then return e end;redis.call("SREM",KEYS[1],ARGV[1]);node_event(KEYS[2],ARGV[5],ARGV[2],"",ARGV[3],ARGV[4],ARGV[1],"0");return "ok"`

const lookupLua = luaHelpers + `
local e=expect_type(KEYS[1],"string","placement") or expect_type(KEYS[2],"string","node") or expect_type(KEYS[3],"zset","leases");if e then return e end
local praw=redis.call("GET",KEYS[1]);if not praw or praw~=ARGV[1] then return "placement_not_found" end;local p,pe=decode(praw,"placement");if pe then return pe end
local nraw=redis.call("GET",KEYS[2]);if not nraw then return "placement_not_found" end;local n,ne=decode(nraw,"node");if ne then return ne end
local score=redis.call("ZSCORE",KEYS[3],ARGV[2]);local now=now_millis()
if p["GrainKey"]~=ARGV[3] or p["Status"]~="active" or n["NodeKey"]~=ARGV[2] or p["NodeIdentity"]~=n["NodeIdentity"] or p["OwnerNodeSessionID"]~=n["NodeSessionID"] or (n["Status"]~="active" and n["Status"]~="draining") or tonumber(n["Lease"]["Version"] or 0)<=0 or tonumber(n["Lease"]["TTLMillis"] or 0)<=0 or not score or tonumber(score)~=tonumber(n["Lease"]["ExpireAtUnixMilli"] or -1) or tonumber(score)<=now then return "placement_not_found" end
return {praw,tostring(n["Lease"]["Version"]),tostring(math.max(0,tonumber(score)-now))}
`

const allocateLua = luaHelpers + `
local e=expect_type(KEYS[1],"string","placement") or expect_type(KEYS[2],"set","nodes") or expect_type(KEYS[3],"set","invalid") or expect_type(KEYS[4],"string","round_robin") or expect_type(KEYS[5],"zset","leases") or expect_type(KEYS[6],"string","sequence") or expect_type(KEYS[7],"stream","events") or expect_type(KEYS[8],"zset","old_index") or expect_type(KEYS[9],"string","old_node") or expect_type(KEYS[10],"zset","owner_leases");if e then return e end
local existing_raw=redis.call("GET",KEYS[1]);local existing,xe=decode(existing_raw,"placement");if xe then return xe end;local now=now_millis()
if existing_raw then if existing_raw~=ARGV[4] then return "conflict" end;if existing["GrainKey"]~=ARGV[3] then return redis.error_reply("IDENTITY_MISMATCH") end;if existing["Status"]=="active" then local nr=redis.call("GET",KEYS[9]);local oldnode;oldnode,xe=decode(nr,"owner_node");if xe then return xe end;local score=oldnode and redis.call("ZSCORE",KEYS[10],KEYS[9]) or nil;if oldnode and oldnode["NodeKey"]==KEYS[9] and existing["NodeIdentity"]==oldnode["NodeIdentity"] and existing["OwnerNodeSessionID"]==oldnode["NodeSessionID"] and (oldnode["Status"]=="active" or oldnode["Status"]=="draining") and score and tonumber(score)==tonumber(oldnode["Lease"]["ExpireAtUnixMilli"] or -1) and tonumber(score)>now then return existing_raw end;return "owner_unavailable" end end
local node_keys=redis.call("SMEMBERS",KEYS[2]);table.sort(node_keys);local candidates={}
for _,key in ipairs(node_keys) do local te=expect_type(key,"string","candidate_node");if te then return te end;local nr=redis.call("GET",key);local n,ne=decode(nr,"candidate_node");if ne then return ne end;if n then local index_error=expect_type(n["PlacementNodeKey"],"zset","candidate_index");if index_error then return index_error end;local score=redis.call("ZSCORE",KEYS[5],key);if n["NodeKey"]==key and n["NodeType"]==ARGV[6] and n["NodeGroup"]==ARGV[7] and n["Status"]=="active" and redis.call("SISMEMBER",KEYS[3],n["NodeName"])==0 and score and tonumber(score)==tonumber(n["Lease"]["ExpireAtUnixMilli"] or -1) and tonumber(score)>now then table.insert(candidates,{key=key,node=n}) end end end
if #candidates==0 then return "no_available_node" end
local rr,re=read_counter(KEYS[4],"round_robin","9223372036854775807");if re then return re end;local seq,se=read_counter(KEYS[6],"sequence","9007199254740991");if se then return se end
local chosen=candidates[decimal_mod(rr,#candidates)+1];local version=existing and tonumber(existing["Version"])+1 or 1;local p={GrainID=ARGV[1],Kind=ARGV[2],GrainKey=ARGV[3],NodeIdentity=chosen.node["NodeIdentity"],OwnerNodeSessionID=chosen.node["NodeSessionID"],Version=version,Status="active",CreateTimeUnixMilli=now,UpdateTimeUnixMilli=now};local encoded=cjson.encode(p)
redis.call("INCR",KEYS[4]);local nextseq=redis.call("INCR",KEYS[6]);if existing then redis.call("ZREM",KEYS[8],ARGV[3]) end;redis.call("SET",KEYS[1],encoded);redis.call("ZADD",chosen.node["PlacementNodeKey"],nextseq,ARGV[3]);event(KEYS[7],ARGV[5],ARGV[3],chosen.node["NodeIdentity"],chosen.node["NodeSessionID"],tostring(version),"0");return encoded
`

const renewPlacementLua = luaHelpers + `
local e=expect_type(KEYS[1],"string","placement") or expect_type(KEYS[2],"string","node") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"stream","audit");if e then return e end
local raw=redis.call("GET",KEYS[1]);if not raw then return "placement_not_found" end;if raw~=ARGV[1] then return "version_conflict" end;local p,pe=decode(raw,"placement");if pe then return pe end
local nr=redis.call("GET",KEYS[2]);local n,ne=decode(nr,"node");if ne then return ne end
if p["GrainKey"]~=ARGV[6] or p["Status"]~="active" then return "placement_not_found" end;if p["NodeIdentity"]~=ARGV[2] then return "invalid_owner" end;if tostring(p["Version"])~=ARGV[4] then return "version_conflict" end;if not n or n["NodeKey"]~=KEYS[2] or n["NodeIdentity"]~=p["NodeIdentity"] or (n["Status"]~="active" and n["Status"]~="draining") then return "owner_unavailable" end;if p["OwnerNodeSessionID"]~=ARGV[3] or n["NodeSessionID"]~=ARGV[3] then return "invalid_node_session" end
local score=redis.call("ZSCORE",KEYS[3],KEYS[2]);if not score or tonumber(score)~=tonumber(n["Lease"]["ExpireAtUnixMilli"] or -1) then return "owner_unavailable" end;if tonumber(score)<=now_millis() then return "node_lease_expired" end
event(KEYS[4],ARGV[5],p["GrainKey"],p["NodeIdentity"],p["OwnerNodeSessionID"],tostring(p["Version"]),"0");return raw
`

const mutationLua = luaHelpers + `
local e=expect_type(KEYS[1],"string","placement") or expect_type(KEYS[2],"string","owner_node") or expect_type(KEYS[3],"string","target_node") or expect_type(KEYS[4],"zset","old_index") or expect_type(KEYS[5],"zset","new_index") or expect_type(KEYS[6],"zset","target_leases") or expect_type(KEYS[7],"stream","events") or expect_type(KEYS[8],"zset","owner_leases") or expect_type(KEYS[9],"set","target_invalid") or expect_type(KEYS[10],"string","sequence");if e then return e end
local raw=redis.call("GET",KEYS[1]);if not raw then return "placement_not_found" end;if raw~=ARGV[2] then return "version_conflict" end;local p,pe=decode(raw,"placement");if pe then return pe end
local ownerraw=redis.call("GET",KEYS[2]);local owner,oe=decode(ownerraw,"owner_node");if oe then return oe end;local targetraw=redis.call("GET",KEYS[3]);local target,te=decode(targetraw,"target_node");if te then return te end
if p["GrainKey"]~=ARGV[8] then return "placement_not_found" end;if ARGV[1]=="recover" and tostring(p["Version"])~=ARGV[6] then return "version_conflict" end;if p["Status"]~="active" then if ARGV[1]=="recover" then return "not_recoverable" end;return "placement_not_found" end;if tostring(p["Version"])~=ARGV[6] then return "version_conflict" end
local seq,se=read_counter(KEYS[10],"sequence","9007199254740991");if se then return se end;local now=now_millis()
if ARGV[1]=="release" then if p["NodeIdentity"]~=ARGV[3] then return "invalid_owner" end;if not owner or owner["NodeKey"]~=KEYS[2] or owner["NodeIdentity"]~=p["NodeIdentity"] or owner["NodeType"]~=ARGV[9] or owner["NodeGroup"]~=ARGV[10] or owner["NodeName"]~=ARGV[11] then return "owner_unavailable" end;if p["OwnerNodeSessionID"]~=ARGV[5] or owner["NodeSessionID"]~=ARGV[5] then return "invalid_node_session" end
else
 if ARGV[1]=="transfer" and ARGV[3]~="" and p["NodeIdentity"]~=ARGV[3] then return "invalid_owner" end
 if ARGV[1]=="recover" then local owner_score=owner and redis.call("ZSCORE",KEYS[8],KEYS[2]) or nil;local healthy=owner and owner["NodeKey"]==KEYS[2] and owner["NodeIdentity"]==p["NodeIdentity"] and owner["NodeSessionID"]==p["OwnerNodeSessionID"] and (owner["Status"]=="active" or owner["Status"]=="draining") and owner_score and tonumber(owner_score)==tonumber(owner["Lease"]["ExpireAtUnixMilli"] or -1) and tonumber(owner_score)>now;if healthy then return "not_recoverable" end end
 if not target or target["NodeKey"]~=KEYS[3] or target["NodeIdentity"]~=ARGV[4] or target["NodeType"]~=ARGV[12] or target["NodeGroup"]~=ARGV[13] or target["NodeName"]~=ARGV[14] or target["PlacementNodeKey"]~=KEYS[5] or target["Status"]~="active" or redis.call("SISMEMBER",KEYS[9],target["NodeName"])~=0 then return "no_available_node" end;local score=redis.call("ZSCORE",KEYS[6],KEYS[3]);if not score or tonumber(score)~=tonumber(target["Lease"]["ExpireAtUnixMilli"] or -1) or tonumber(score)<=now then return "no_available_node" end
end
p["Version"]=tonumber(p["Version"])+1;p["UpdateTimeUnixMilli"]=now;if ARGV[1]=="release" then p["Status"]="released" else p["NodeIdentity"]=target["NodeIdentity"];p["OwnerNodeSessionID"]=target["NodeSessionID"] end;local encoded=cjson.encode(p)
redis.call("ZREM",KEYS[4],p["GrainKey"]);redis.call("SET",KEYS[1],encoded);if ARGV[1]~="release" then local score=redis.call("INCR",KEYS[10]);redis.call("ZADD",KEYS[5],score,p["GrainKey"]) end;event(KEYS[7],ARGV[7],p["GrainKey"],p["NodeIdentity"],p["OwnerNodeSessionID"],tostring(p["Version"]),"0");return encoded
`

const (
	ScriptAllocate        = "allocate"
	ScriptRenew           = "renew"
	ScriptRelease         = "release"
	ScriptTransfer        = "transfer"
	ScriptRecover         = "recover"
	ScriptDrain           = "drain"
	ScriptNodeLeaseExpire = "node_lease_expire"
)

type ScriptSpec struct {
	Name              string
	WritesOutbox      bool
	WritesAuditStream bool
}

func ScriptSpecs() []ScriptSpec {
	return []ScriptSpec{{ScriptAllocate, true, false}, {ScriptRenew, false, true}, {ScriptRelease, true, false}, {ScriptTransfer, true, false}, {ScriptRecover, true, false}, {ScriptDrain, true, false}, {ScriptNodeLeaseExpire, true, false}}
}
