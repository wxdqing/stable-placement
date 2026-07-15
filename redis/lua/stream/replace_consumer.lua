
local groups = redis.call("XINFO", "GROUPS", KEYS[1])
local old_found = false
local new_found = false

for _, group in ipairs(groups) do
	local name = nil
	local pending = nil
	local lag = nil
	for index = 1, #group, REDIS_FIELD_PAIR_STEP do
		if group[index] == "name" then
			name = group[index + REDIS_FIELD_VALUE_OFFSET]
		elseif group[index] == "pending" then
			pending = group[index + REDIS_FIELD_VALUE_OFFSET]
		elseif group[index] == "lag" then
			lag = group[index + REDIS_FIELD_VALUE_OFFSET]
		end
	end
	if name == ARGV[1] then
		old_found = true
		if type(pending) ~= "number" or pending ~= 0 then
			return SCRIPT_RESULT_PENDING
		end
		if type(lag) ~= "number" or lag ~= 0 then
			return SCRIPT_RESULT_PENDING
		end
	elseif name == ARGV[2] then
		new_found = true
	end
end

if not old_found then
	if new_found then
		return SCRIPT_RESULT_OK
	end
	return redis.error_reply("NOGROUP old consumer group does not exist")
end

if not new_found then
	redis.call("XGROUP", "CREATE", KEYS[1], ARGV[2], "$")
end
redis.call("XGROUP", "DESTROY", KEYS[1], ARGV[1])
return SCRIPT_RESULT_OK
