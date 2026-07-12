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
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then
		actual = actual["ok"]
	end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end

local function read_counter(key, label, maximum)
	local raw = redis.call("GET", key)
	if not raw then
		return "0", nil
	end
	if not string.match(raw, "^%d+$") then
		return nil, redis.error_reply("INVALID_COUNTER " .. label .. " must be a non-negative decimal")
	end
	if #raw > 1 and string.sub(raw, 1, 1) == "0" then
		return nil, redis.error_reply("INVALID_COUNTER " .. label .. " must not contain leading zeros")
	end
	if #raw > #maximum or (#raw == #maximum and raw >= maximum) then
		return nil, redis.error_reply("INVALID_COUNTER " .. label .. " must be less than " .. maximum)
	end
	return raw, nil
end

local function decimal_mod(value, divisor)
	local remainder = 0
	for index = 1, #value do
		remainder = (remainder * 10 + tonumber(string.sub(value, index, index))) % divisor
	end
	return remainder
end

local type_error = expect_type(KEYS[1], "string", "placement")
	or expect_type(KEYS[2], "set", "nodes")
	or expect_type(KEYS[3], "set", "invalid_nodes")
	or expect_type(KEYS[4], "string", "round_robin")
	or expect_type(KEYS[5], "zset", "lease_expire")
	or expect_type(KEYS[6], "string", "sequence")
	or expect_type(KEYS[7], "stream", "events")
	or expect_type(KEYS[8], "zset", "old_node_index")
if type_error then
	return type_error
end

local existing = redis.call("GET", KEYS[1])
local next_version = 1
local remove_old_node = false
if existing then
	local existing_placement = cjson.decode(existing)
	if existing_placement["Status"] == "active" then
		if ARGV[9] ~= "" and existing ~= ARGV[9] then
			return "conflict"
		end
		local expire_at = redis.call("ZSCORE", KEYS[5], ARGV[3])
		if not expire_at or tonumber(expire_at) > tonumber(ARGV[8]) then
			return existing
		end
		if existing ~= ARGV[9] then
			return "conflict"
		end
		remove_old_node = true
	elseif existing ~= ARGV[9] then
		return "conflict"
	end
	next_version = tonumber(existing_placement["Version"] or 0) + 1
elseif ARGV[9] ~= "" then
	return "conflict"
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

local cursor, counter_error = read_counter(KEYS[4], "round_robin", "9223372036854775807")
if counter_error then
	return counter_error
end
local _, sequence_error = read_counter(KEYS[6], "sequence", "9007199254740991")
if sequence_error then
	return sequence_error
end
local chosen = effective[decimal_mod(cursor, #effective) + 1]
type_error = expect_type(chosen["PlacementNodeKey"], "zset", "new_node_index")
if type_error then
	return type_error
end
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
if remove_old_node then
	redis.call("ZREM", KEYS[8], ARGV[3])
end
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
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "string", "placement")
	or expect_type(KEYS[2], "zset", "lease_expire")
	or expect_type(KEYS[3], "stream", "audit")
	or expect_type(KEYS[4], "string", "node")
if type_error then return type_error end

local node_raw = redis.call("GET", KEYS[4])
if not node_raw then
	return "invalid_node_session"
end
local node = cjson.decode(node_raw)
if node["NodeSessionID"] ~= ARGV[9] then
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
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "string", "node")
	or expect_type(KEYS[2], "zset", "node_heartbeat")
if type_error then return type_error end
local raw = redis.call("GET", KEYS[1])
if not raw then return "node_not_found" end
local node = cjson.decode(raw)
if node["Status"] ~= "active" then return "node_not_found" end
if node["NodeSessionID"] ~= ARGV[1] then return "invalid_node_session" end
node["LastHeartbeatAt"] = ARGV[2]
local updated = cjson.encode(node)
redis.call("SET", KEYS[1], updated)
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
return updated
`

const mutationLua = `
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end

local function validate_sequence_counter(key)
	local raw = redis.call("GET", key)
	if not raw then return nil end
	if not string.match(raw, "^%d+$") then
		return redis.error_reply("INVALID_COUNTER sequence must be a non-negative decimal")
	end
	if #raw > 1 and string.sub(raw, 1, 1) == "0" then
		return redis.error_reply("INVALID_COUNTER sequence must not contain leading zeros")
	end
	local maximum = "9007199254740991"
	if #raw > #maximum or (#raw == #maximum and raw >= maximum) then
		return redis.error_reply("INVALID_COUNTER sequence must be less than " .. maximum)
	end
	return nil
end

local type_error = expect_type(KEYS[1], "string", "placement")
	or expect_type(KEYS[6], "stream", "events")
if not type_error and ARGV[3] == "1" then type_error = expect_type(KEYS[2], "zset", "old_node_index") end
if not type_error and ARGV[4] == "1" then
	type_error = expect_type(KEYS[3], "zset", "new_node_index")
		or expect_type(KEYS[5], "string", "sequence")
end
if not type_error and (ARGV[5] == "remove" or ARGV[5] == "add") then type_error = expect_type(KEYS[4], "zset", "lease_expire") end
if not type_error and ARGV[12] == "1" then type_error = expect_type(KEYS[7], "string", "node") end
if not type_error and ARGV[14] == "1" then
	type_error = expect_type(KEYS[8], "string", "target_node")
		or expect_type(KEYS[9], "set", "invalid_nodes")
end
if type_error then return type_error end

if ARGV[12] == "1" then
	local node_raw = redis.call("GET", KEYS[7])
	if not node_raw then
		return "invalid_node_session"
	end
	local node = cjson.decode(node_raw)
	if node["NodeSessionID"] ~= ARGV[13] then
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

if ARGV[4] == "1" then
	local sequence_error = validate_sequence_counter(KEYS[5])
	if sequence_error then return sequence_error end
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
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "string", "node")
	or expect_type(KEYS[2], "set", "nodes")
	or expect_type(KEYS[3], "stream", "events")
	or expect_type(KEYS[4], "zset", "node_heartbeat")
if type_error then return type_error end
redis.call("SET", KEYS[1], ARGV[1])
redis.call("SADD", KEYS[2], ARGV[7])
redis.call("ZADD", KEYS[4], ARGV[8], ARGV[7])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[3],
	"node_identity", ARGV[2],
	"node_type", ARGV[4],
	"node_group", ARGV[5],
	"node_name", ARGV[6])
return ARGV[1]
`

const replaceNodeSessionLua = `
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "string", "node")
	or expect_type(KEYS[2], "set", "nodes")
	or expect_type(KEYS[3], "stream", "events")
	or expect_type(KEYS[4], "zset", "node_heartbeat")
if type_error then return type_error end
local old = redis.call("GET", KEYS[1])
redis.call("SET", KEYS[1], ARGV[1])
redis.call("SADD", KEYS[2], ARGV[7])
redis.call("ZADD", KEYS[4], ARGV[8], ARGV[7])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[3],
	"node_identity", ARGV[2],
	"node_type", ARGV[4],
	"node_group", ARGV[5],
	"node_name", ARGV[6])
return old or ""
`

const markNodeInvalidLua = `
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "set", "invalid_nodes")
	or expect_type(KEYS[2], "stream", "events")
if type_error then return type_error end
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
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "set", "invalid_nodes")
	or expect_type(KEYS[2], "stream", "events")
if type_error then return type_error end
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
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "string", "node")
	or expect_type(KEYS[2], "set", "invalid_nodes")
	or expect_type(KEYS[3], "stream", "events")
if type_error then return type_error end
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
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "string", "node")
	or expect_type(KEYS[2], "set", "nodes")
	or expect_type(KEYS[3], "stream", "events")
	or expect_type(KEYS[4], "zset", "node_placements")
	or expect_type(KEYS[5], "zset", "node_heartbeat")
if type_error then return type_error end
local node_raw = redis.call("GET", KEYS[1])
if not node_raw then
	return "node_not_found"
end
local node = cjson.decode(node_raw)
if node["NodeSessionID"] ~= ARGV[1] then
	return "invalid_node_session"
end
if ARGV[3] == "1" and redis.call("ZCARD", KEYS[4]) > 0 then
	return "node_has_placements"
end
redis.call("DEL", KEYS[1])
redis.call("SREM", KEYS[2], KEYS[1])
redis.call("ZREM", KEYS[5], KEYS[1])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[2],
	"node_identity", node["NodeIdentity"],
	"node_type", node["NodeType"],
	"node_group", node["NodeGroup"],
	"node_name", node["NodeName"])
return "ok"
`

const expireHeartbeatLua = `
local function expect_type(key, expected, label)
	local actual = redis.call("TYPE", key)
	if type(actual) == "table" then actual = actual["ok"] end
	if actual ~= "none" and actual ~= expected then
		return redis.error_reply("WRONGTYPE " .. label .. " expected " .. expected .. " got " .. actual)
	end
	return nil
end
local type_error = expect_type(KEYS[1], "string", "node")
	or expect_type(KEYS[2], "zset", "node_heartbeat")
	or expect_type(KEYS[3], "stream", "events")
if type_error then return type_error end

local score = redis.call("ZSCORE", KEYS[2], ARGV[1])
if not score or tonumber(score) ~= tonumber(ARGV[2]) or tonumber(score) > tonumber(ARGV[3]) then
	return 0
end
local raw = redis.call("GET", KEYS[1])
if not raw then
	redis.call("ZREM", KEYS[2], ARGV[1])
	return 0
end
if raw ~= ARGV[4] then return 0 end
local node = cjson.decode(raw)
if (node["Status"] ~= "active" and node["Status"] ~= "draining") or node["NodeType"] ~= ARGV[6] or node["NodeGroup"] ~= ARGV[7] then
	redis.call("ZREM", KEYS[2], ARGV[1])
	return 0
end
if node["NodeSessionID"] ~= ARGV[5] then return 0 end
node["Status"] = "offline"
redis.call("SET", KEYS[1], cjson.encode(node))
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("XADD", KEYS[3], "*",
	"type", ARGV[8],
	"node_identity", node["NodeIdentity"],
	"node_type", node["NodeType"],
	"node_group", node["NodeGroup"],
	"node_name", node["NodeName"])
return 1
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
