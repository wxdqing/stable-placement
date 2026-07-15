local function event(stream, kind, grain, placement_id, node, session, placement_version, lease_version)
  redis.call("XADD", stream, "*", "type", kind, "grain_key", grain or "", "placement_id", placement_id or "", "node_identity", node or "", "node_session_id", session or "", "node_type", "", "node_group", "", "node_name", "", "placement_version", placement_version or NO_VERSION_STRING, "node_lease_version", lease_version or NO_VERSION_STRING)
end

local function node_event(stream, kind, node, session, node_type, node_group, node_name, lease_version)
  redis.call("XADD", stream, "*", "type", kind, "grain_key", "", "placement_id", "", "node_identity", node or "", "node_session_id", session or "", "node_type", node_type or "", "node_group", node_group or "", "node_name", node_name or "", "placement_version", NO_VERSION_STRING, "node_lease_version", lease_version or NO_VERSION_STRING)
end
