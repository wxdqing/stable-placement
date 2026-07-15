
local e=expect_type(KEYS[1],"string","placement") or expect_type(KEYS[2],"string","node") or expect_type(KEYS[3],"zset","leases") or expect_type(KEYS[4],"stream","audit");if e then return e end
local raw=redis.call("GET",KEYS[1]);if not raw then return "placement_not_found" end;if raw~=ARGV[1] then return "version_conflict" end;local p,pe=decode(raw,"placement");if pe then return pe end
local nr=redis.call("GET",KEYS[2]);local n,ne=decode(nr,"node");if ne then return ne end
if p["GrainKey"]~=ARGV[6] or p["Status"]~="active" then return "placement_not_found" end;if p["NodeIdentity"]~=ARGV[2] then return "invalid_owner" end;if p["PlacementID"]~=ARGV[7] or tostring(p["Version"])~=ARGV[4] then return "version_conflict" end;if not n or n["NodeKey"]~=KEYS[2] or n["NodeIdentity"]~=p["NodeIdentity"] or (n["Status"]~="active" and n["Status"]~="draining") then return "owner_unavailable" end;if p["OwnerNodeSessionID"]~=ARGV[3] or n["NodeSessionID"]~=ARGV[3] then return "invalid_node_session" end
local score=redis.call("ZSCORE",KEYS[3],KEYS[2]);if not score or tonumber(score)~=tonumber(n["Lease"]["ExpireAtUnixMilli"] or -1) then return "owner_unavailable" end;if tonumber(score)<=now_millis() then return "node_lease_expired" end
event(KEYS[4],ARGV[5],p["GrainKey"],p["PlacementID"],p["NodeIdentity"],p["OwnerNodeSessionID"],tostring(p["Version"]),NO_VERSION_STRING);return raw
