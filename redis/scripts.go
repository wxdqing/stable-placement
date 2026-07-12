package redis

const (
	ScriptAllocate = "allocate"
	ScriptRenew    = "renew"
	ScriptRelease  = "release"
	ScriptTransfer = "transfer"
	ScriptRecover  = "recover"
	ScriptExpire   = "expire"
	ScriptDrain    = "drain"
)

const allocateLua = `
local existing = redis.call("GET", KEYS[1])
local next_version = 1
if existing then
	local existing_placement = cjson.decode(existing)
	if existing_placement["Status"] == "active" then
		return existing
	end
	next_version = tonumber(existing_placement["Version"] or 0) + 1
end

local candidate_keys = redis.call("SMEMBERS", KEYS[2])
table.sort(candidate_keys)
local effective = {}
for _, candidate_key in ipairs(candidate_keys) do
	local node_raw = redis.call("GET", candidate_key)
	if node_raw then
		local node = cjson.decode(node_raw)
		if node["Status"] == "active" and redis.call("SISMEMBER", KEYS[3], node["NodeName"]) == 0 then
			table.insert(effective, node)
		end
	end
end
if #effective == 0 then
	return "no_available_node"
end

local cursor = tonumber(redis.call("GET", KEYS[4]) or "0")
local chosen = effective[(cursor % #effective) + 1]
local placement = {
	GrainID = ARGV[1],
	Kind = ARGV[2],
	GrainKey = ARGV[3],
	NodeIdentity = chosen["NodeIdentity"],
	Version = next_version,
	Status = "active",
	CreateTime = ARGV[4],
	UpdateTime = ARGV[4],
	LeaseExpireAt = ARGV[5],
	Lease = {
		OwnerNodeIdentity = chosen["NodeIdentity"],
		OwnerNodeSessionID = chosen["NodeSessionID"],
		Version = 1,
		ExpireAt = ARGV[5],
	},
}
local placement_raw = cjson.encode(placement)
redis.call("SET", KEYS[1], placement_raw)
redis.call("INCR", KEYS[4])
local score = redis.call("INCR", KEYS[6])
redis.call("ZADD", chosen["PlacementNodeKey"], score, ARGV[3])
redis.call("ZADD", KEYS[5], ARGV[6], ARGV[3])
redis.call("XADD", KEYS[7], "*",
	"type", ARGV[7],
	"grain_key", ARGV[3],
	"node_identity", chosen["NodeIdentity"],
	"placement_version", tostring(next_version),
	"lease_version", "1")
return placement_raw
`

const renewLua = `
local node = redis.call("GET", KEYS[4])
if not node then
	return "invalid_node_session"
end
if not string.find(node, '"NodeSessionID":"' .. ARGV[9] .. '"', 1, true) then
	return "invalid_node_session"
end

if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return "conflict"
end

redis.call("SET", KEYS[1], ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[5],
	"grain_key", ARGV[4],
	"node_identity", ARGV[6],
	"placement_version", ARGV[7],
	"lease_version", ARGV[8])
return ARGV[2]
`

const renewNodeLua = `
local raw = redis.call("GET", KEYS[1])
if not raw then return "node_not_found" end
local node = cjson.decode(raw)
if node["NodeSessionID"] ~= ARGV[1] then return "invalid_node_session" end
node["LastHeartbeatAt"] = ARGV[2]
local updated = cjson.encode(node)
redis.call("SET", KEYS[1], updated)
return updated
`

const mutationLua = `
if ARGV[12] == "1" then
	local node = redis.call("GET", KEYS[7])
	if not node then
		return "invalid_node_session"
	end
	if not string.find(node, '"NodeSessionID":"' .. ARGV[13] .. '"', 1, true) then
		return "invalid_node_session"
	end
end

local updated_placement = ARGV[2]
local event_node_identity = ARGV[9]
if ARGV[14] == "1" then
	local target_raw = redis.call("GET", KEYS[8])
	if not target_raw then
		return "no_available_node"
	end
	local target = cjson.decode(target_raw)
	if target["NodeType"] ~= ARGV[15] or target["NodeGroup"] ~= ARGV[16] or target["NodeName"] ~= ARGV[17] then
		return "no_available_node"
	end
	if target["Status"] ~= "active" or redis.call("SISMEMBER", KEYS[9], ARGV[17]) ~= 0 then
		return "no_available_node"
	end
	local placement = cjson.decode(updated_placement)
	placement["NodeIdentity"] = target["NodeIdentity"]
	placement["Lease"]["OwnerNodeIdentity"] = target["NodeIdentity"]
	placement["Lease"]["OwnerNodeSessionID"] = target["NodeSessionID"]
	updated_placement = cjson.encode(placement)
	event_node_identity = target["NodeIdentity"]
end

if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return "conflict"
end

redis.call("SET", KEYS[1], updated_placement)
if ARGV[3] == "1" then
	redis.call("ZREM", KEYS[2], ARGV[6])
end
if ARGV[4] == "1" then
	local score = redis.call("INCR", KEYS[5])
	redis.call("ZADD", KEYS[3], score, ARGV[6])
end
if ARGV[5] == "remove" then
	redis.call("ZREM", KEYS[4], ARGV[6])
elseif ARGV[5] == "add" then
	redis.call("ZADD", KEYS[4], ARGV[7], ARGV[6])
end
redis.call("XADD", KEYS[6], "*",
	"type", ARGV[8],
	"grain_key", ARGV[6],
	"node_identity", event_node_identity,
	"placement_version", ARGV[10],
	"lease_version", ARGV[11])
return updated_placement
`

const registerNodeLua = `
redis.call("SET", KEYS[1], ARGV[1])
redis.call("SADD", KEYS[2], ARGV[7])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[3],
	"node_identity", ARGV[2],
	"node_type", ARGV[4],
	"node_group", ARGV[5],
	"node_name", ARGV[6])
return ARGV[1]
`

const replaceNodeSessionLua = `
local old = redis.call("GET", KEYS[1])
redis.call("SET", KEYS[1], ARGV[1])
redis.call("SADD", KEYS[2], ARGV[7])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[3],
	"node_identity", ARGV[2],
	"node_type", ARGV[4],
	"node_group", ARGV[5],
	"node_name", ARGV[6])
return old or ""
`

const markNodeInvalidLua = `
redis.call("SADD", KEYS[1], ARGV[1])
redis.call("XADD", KEYS[2], "*",
	"type", ARGV[2],
	"node_identity", ARGV[3],
	"node_type", ARGV[4],
	"node_group", ARGV[5],
	"node_name", ARGV[1])
return "ok"
`

const restoreNodeLua = `
redis.call("SREM", KEYS[1], ARGV[1])
redis.call("XADD", KEYS[2], "*",
	"type", ARGV[2],
	"node_identity", ARGV[3],
	"node_type", ARGV[4],
	"node_group", ARGV[5],
	"node_name", ARGV[1])
return "ok"
`

const drainNodeLua = `
local node_raw = redis.call("GET", KEYS[1])
if not node_raw then
	return "node_not_found"
end
local node = cjson.decode(node_raw)
if redis.call("SISMEMBER", KEYS[2], node["NodeName"]) == 0 then
	return "node_not_invalid"
end
node["Status"] = "draining"
local updated = cjson.encode(node)
redis.call("SET", KEYS[1], updated)
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[1],
	"node_identity", node["NodeIdentity"],
	"node_type", node["NodeType"],
	"node_group", node["NodeGroup"],
	"node_name", node["NodeName"])
return updated
`

const unregisterNodeLua = `
local node_raw = redis.call("GET", KEYS[1])
if not node_raw then
	return "node_not_found"
end
local node = cjson.decode(node_raw)
if node["NodeSessionID"] ~= ARGV[1] then
	return "invalid_node_session"
end
redis.call("DEL", KEYS[1])
redis.call("SREM", KEYS[2], KEYS[1])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[2],
	"node_identity", node["NodeIdentity"],
	"node_type", node["NodeType"],
	"node_group", node["NodeGroup"],
	"node_name", node["NodeName"])
return "ok"
`

type ScriptSpec struct {
	Name              string
	WritesOutbox      bool
	WritesAuditStream bool
}

func ScriptSpecs() []ScriptSpec {
	return []ScriptSpec{
		{Name: ScriptAllocate, WritesOutbox: true},
		{Name: ScriptRenew, WritesAuditStream: true},
		{Name: ScriptRelease, WritesOutbox: true},
		{Name: ScriptTransfer, WritesOutbox: true},
		{Name: ScriptRecover, WritesOutbox: true},
		{Name: ScriptExpire, WritesOutbox: true},
		{Name: ScriptDrain, WritesOutbox: true},
	}
}
