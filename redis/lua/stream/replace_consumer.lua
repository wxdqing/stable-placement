
local groups = redis.call("XINFO", "GROUPS", KEYS[1])
local old_found = false
local new_found = false

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
		old_found = true
		if type(pending) ~= "number" or pending ~= 0 then
			return 1
		end
		if type(lag) ~= "number" or lag ~= 0 then
			return 1
		end
	elseif name == ARGV[2] then
		new_found = true
	end
end

if not old_found then
	if new_found then
		return 0
	end
	return redis.error_reply("NOGROUP old consumer group does not exist")
end

if not new_found then
	redis.call("XGROUP", "CREATE", KEYS[1], ARGV[2], "$")
end
redis.call("XGROUP", "DESTROY", KEYS[1], ARGV[1])
return 0

