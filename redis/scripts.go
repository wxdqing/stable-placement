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
if existing and string.find(existing, '"Status":"active"', 1, true) then
	return existing
end

redis.call("SET", KEYS[1], ARGV[1])
local score = redis.call("INCR", KEYS[4])
redis.call("ZADD", KEYS[2], score, ARGV[2])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[2])
redis.call("XADD", KEYS[5], "*",
	"type", ARGV[4],
	"grain_key", ARGV[2],
	"node_identity", ARGV[5],
	"placement_version", ARGV[6],
	"lease_version", ARGV[7])
return ARGV[1]
`

const renewLua = `
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

const mutationLua = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return "conflict"
end

redis.call("SET", KEYS[1], ARGV[2])
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
	"node_identity", ARGV[9],
	"placement_version", ARGV[10],
	"lease_version", ARGV[11])
return ARGV[2]
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
