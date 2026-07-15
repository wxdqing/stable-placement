
local e=expect_type(KEYS[1],"string","placement") or expect_type(KEYS[2],"set","nodes") or expect_type(KEYS[3],"set","invalid") or expect_type(KEYS[4],"string","round_robin") or expect_type(KEYS[5],"zset","leases") or expect_type(KEYS[6],"string","sequence") or expect_type(KEYS[7],"stream","events") or expect_type(KEYS[8],"zset","old_index") or expect_type(KEYS[9],"string","owner_node") or expect_type(KEYS[10],"zset","owner_leases");if e then return e end
local existing_raw=redis.call("GET",KEYS[1]);if (existing_raw or "")~=ARGV[4] then return "conflict" end
local existing,xe=decode(existing_raw,"placement");if xe then return xe end;local now=now_millis()
if existing and existing["GrainKey"]~=ARGV[3] then return redis.error_reply("IDENTITY_MISMATCH") end
if existing and existing["Status"]=="active" then
  if ARGV[9]~=ARGV[6] or ARGV[10]~=ARGV[7] then return "target_mismatch" end
  local owner_raw=redis.call("GET",KEYS[9]);local owner,oe=decode(owner_raw,"owner_node");if oe then return oe end
  local owner_score=owner and redis.call("ZSCORE",KEYS[10],KEYS[9]) or nil
  local owner_usable=owner and owner["NodeKey"]==KEYS[9] and owner["NodeIdentity"]==existing["NodeIdentity"] and owner["NodeType"]==ARGV[9] and owner["NodeGroup"]==ARGV[10] and owner["NodeName"]==ARGV[11] and (owner["Status"]=="active" or owner["Status"]=="draining") and tonumber(owner["Lease"]["Version"] or 0)>0 and owner_score and tonumber(owner_score)==tonumber(owner["Lease"]["ExpireAtUnixMilli"] or -1) and tonumber(owner_score)>now
  if owner_usable and owner["NodeSessionID"]==existing["OwnerNodeSessionID"] then return {existing_raw,tostring(owner["Lease"]["Version"]),tostring(tonumber(owner_score)-now)} end
  if owner_usable and owner["NodeSessionID"]~=existing["OwnerNodeSessionID"] then
    existing["OwnerNodeSessionID"]=owner["NodeSessionID"];existing["Version"]=tonumber(existing["Version"])+VERSION_INCREMENT;existing["UpdateTimeUnixMilli"]=now
    local encoded=cjson.encode(existing);redis.call("SET",KEYS[1],encoded);event(KEYS[7],ARGV[8],ARGV[3],existing["PlacementID"],owner["NodeIdentity"],owner["NodeSessionID"],tostring(existing["Version"]),tostring(owner["Lease"]["Version"]))
    return {encoded,tostring(owner["Lease"]["Version"]),tostring(tonumber(owner_score)-now)}
  end
  if redis.call("SISMEMBER",KEYS[3],ARGV[11])==0 then return "owner_unavailable" end
end
local node_keys=redis.call("SMEMBERS",KEYS[2]);table.sort(node_keys);local candidates={}
for _,key in ipairs(node_keys) do
  local te=expect_type(key,"string","candidate_node");if te then return te end
  local nr=redis.call("GET",key);local n,ne=decode(nr,"candidate_node");if ne then return ne end
  if n then
    local index_error=expect_type(n["PlacementNodeKey"],"zset","candidate_index");if index_error then return index_error end
    local score=redis.call("ZSCORE",KEYS[5],key)
    if n["NodeKey"]==key and n["NodeType"]==ARGV[6] and n["NodeGroup"]==ARGV[7] and n["Status"]=="active" and tonumber(n["Lease"]["Version"] or 0)>0 and redis.call("SISMEMBER",KEYS[3],n["NodeName"])==0 and score and tonumber(score)==tonumber(n["Lease"]["ExpireAtUnixMilli"] or -1) and tonumber(score)>now then table.insert(candidates,{key=key,node=n,score=tonumber(score)}) end
  end
end
if #candidates==0 then return "no_available_node" end
local rr,re=read_counter(KEYS[4],"round_robin",MAX_SIGNED_INT64_STRING);if re then return re end
local seq,se=read_counter(KEYS[6],"sequence",MAX_EXACT_LUA_INTEGER_STRING);if se then return se end
local chosen=choose_candidate(candidates,rr,ARGV[12],now,tonumber(ARGV[13]),tonumber(ARGV[14]),tonumber(ARGV[15]),tonumber(ARGV[16]));if not chosen then return "no_available_node" end
local recovering=existing and existing["Status"]=="active"
local version=existing and tonumber(existing["Version"])+VERSION_INCREMENT or INITIAL_PLACEMENT_VERSION
local p
if recovering then
  p=existing;p["NodeIdentity"]=chosen.node["NodeIdentity"];p["OwnerNodeSessionID"]=chosen.node["NodeSessionID"];p["Version"]=version;p["UpdateTimeUnixMilli"]=now
else
  p={GrainID=ARGV[1],Kind=ARGV[2],GrainKey=ARGV[3],PlacementID=ARGV[17],NodeIdentity=chosen.node["NodeIdentity"],OwnerNodeSessionID=chosen.node["NodeSessionID"],Version=INITIAL_PLACEMENT_VERSION,Status="active",CreateTimeUnixMilli=now,UpdateTimeUnixMilli=now};version=INITIAL_PLACEMENT_VERSION
end
local encoded=cjson.encode(p)
redis.call("INCR",KEYS[4]);local nextseq=redis.call("INCR",KEYS[6]);if existing then redis.call("ZREM",KEYS[8],ARGV[3]) end
redis.call("SET",KEYS[1],encoded);redis.call("ZADD",chosen.node["PlacementNodeKey"],nextseq,ARGV[3])
local event_type=recovering and ARGV[8] or ARGV[5]
event(KEYS[7],event_type,ARGV[3],p["PlacementID"],chosen.node["NodeIdentity"],chosen.node["NodeSessionID"],tostring(version),tostring(chosen.node["Lease"]["Version"]))
return {encoded,tostring(chosen.node["Lease"]["Version"]),tostring(chosen.score-now)}
