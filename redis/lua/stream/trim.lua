
if redis.call("EXISTS", KEYS[1]) == 0 then
	return SCRIPT_RESULT_OK
end

local groups = redis.call("XINFO", "GROUPS", KEYS[1])
for _, group in ipairs(groups) do
	local pending = nil
	local lag = nil
	for index = 1, #group, REDIS_FIELD_PAIR_STEP do
		if group[index] == "pending" then
			pending = group[index + REDIS_FIELD_VALUE_OFFSET]
		elseif group[index] == "lag" then
			lag = group[index + REDIS_FIELD_VALUE_OFFSET]
		end
	end
	if type(pending) ~= "number" or pending > 0 then
		return SCRIPT_RESULT_OK
	end
	if type(lag) ~= "number" or lag ~= 0 then
		return SCRIPT_RESULT_OK
	end
end

return redis.call("XTRIM", KEYS[1], "MAXLEN", "=", ARGV[1])
