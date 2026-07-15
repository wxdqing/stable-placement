
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end

local groups = redis.call("XINFO", "GROUPS", KEYS[1])
for _, group in ipairs(groups) do
	local pending = nil
	local lag = nil
	for index = 1, #group, 2 do
		if group[index] == "pending" then
			pending = group[index + 1]
		elseif group[index] == "lag" then
			lag = group[index + 1]
		end
	end
	if type(pending) ~= "number" or pending > 0 then
		return 0
	end
	if type(lag) ~= "number" or lag ~= 0 then
		return 0
	end
end

return redis.call("XTRIM", KEYS[1], "MAXLEN", "=", ARGV[1])

