
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local groups = redis.call("XINFO", "GROUPS", KEYS[1])
for _, group in ipairs(groups) do
	local name = nil
	local pending = nil
	local lag = nil
	for index = 1, #group, 2 do
		if group[index] == "name" then
			name = group[index + 1]
		elseif group[index] == "pending" then
			pending = group[index + 1]
		elseif group[index] == "lag" then
			lag = group[index + 1]
		end
	end
	if name == ARGV[1] then
		if type(pending) ~= "number" or pending ~= 0 then
			return 1
		end
		if type(lag) ~= "number" or lag ~= 0 then
			return 1
		end
		redis.call("XGROUP", "DESTROY", KEYS[1], ARGV[1])
		return 0
	end
end
return 0

