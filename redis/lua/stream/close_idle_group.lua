
if redis.call("EXISTS", KEYS[1]) == 0 then
	return SCRIPT_RESULT_OK
end
local groups = redis.call("XINFO", "GROUPS", KEYS[1])
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
		if type(pending) ~= "number" or pending ~= 0 then
			return SCRIPT_RESULT_PENDING
		end
		if type(lag) ~= "number" or lag ~= 0 then
			return SCRIPT_RESULT_PENDING
		end
		redis.call("XGROUP", "DESTROY", KEYS[1], ARGV[1])
		return SCRIPT_RESULT_OK
	end
end
return SCRIPT_RESULT_OK
