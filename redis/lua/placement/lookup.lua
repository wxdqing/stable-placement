
local e=expect_type(KEYS[1],"string","placement") or expect_type(KEYS[2],"string","node") or expect_type(KEYS[3],"zset","leases");if e then return e end
local praw=redis.call("GET",KEYS[1]);if not praw or praw~=ARGV[1] then return "placement_not_found" end;local p,pe=decode(praw,"placement");if pe then return pe end
local nraw=redis.call("GET",KEYS[2]);if not nraw then return "placement_not_found" end;local n,ne=decode(nraw,"node");if ne then return ne end
local score=redis.call("ZSCORE",KEYS[3],ARGV[2]);local now=now_millis()
if p["GrainKey"]~=ARGV[3] or p["Status"]~="active" or n["NodeKey"]~=ARGV[2] or p["NodeIdentity"]~=n["NodeIdentity"] or p["OwnerNodeSessionID"]~=n["NodeSessionID"] or (n["Status"]~="active" and n["Status"]~="draining") or tonumber(n["Lease"]["Version"] or 0)<=0 or tonumber(n["Lease"]["TTLMillis"] or 0)<=0 or not score or tonumber(score)~=tonumber(n["Lease"]["ExpireAtUnixMilli"] or -1) or tonumber(score)<=now then return "placement_not_found" end
return {praw,tostring(n["Lease"]["Version"]),tostring(math.max(0,tonumber(score)-now))}

