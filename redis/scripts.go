package redis

import (
	"embed"
	"fmt"
	"strings"
)

//go:embed lua
var luaFiles embed.FS

func mustLua(paths ...string) string {
	parts := make([]string, 0, len(paths))
	for _, path := range paths {
		content, err := luaFiles.ReadFile(path)
		if err != nil {
			panic(fmt.Sprintf("read embedded Lua %q: %v", path, err))
		}
		parts = append(parts, string(content))
	}
	return strings.Join(parts, "\n")
}

var (
	registerNodeLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/node/register.lua",
	)
	renewNodeLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/schedule.lua",
		"lua/node/renew.lua",
	)
	replaceNodeSessionLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/node/replace_session.lua",
	)
	expireNodeLeaseLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/lease/expire_node.lua",
	)
	unregisterNodeLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/node/unregister.lua",
	)
	drainNodeLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/node/drain.lua",
	)
	markInvalidLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/node/mark_invalid.lua",
	)
	restoreNodeLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/node/restore.lua",
	)
	lookupLua = mustLua(
		"lua/shared/helper.lua",
		"lua/placement/lookup.lua",
	)
	allocateLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/shared/schedule.lua",
		"lua/placement/allocate.lua",
	)
	resolveRouteLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/shared/schedule.lua",
		"lua/placement/resolve_route.lua",
	)
	renewPlacementLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/placement/renew.lua",
	)
	mutationLua = mustLua(
		"lua/shared/helper.lua",
		"lua/shared/event.lua",
		"lua/placement/mutate.lua",
	)
	trimStreamLua               = mustLua("lua/stream/trim.lua")
	replaceConsumerLua          = mustLua("lua/stream/replace_consumer.lua")
	closeConsumerGroupIfIdleLua = mustLua("lua/stream/close_idle_group.lua")
)

const (
	ScriptAllocate        = "allocate"
	ScriptRenew           = "renew"
	ScriptRelease         = "release"
	ScriptTransfer        = "transfer"
	ScriptRecover         = "recover"
	ScriptDrain           = "drain"
	ScriptNodeLeaseExpire = "node_lease_expire"
)

type ScriptSpec struct {
	Name              string
	WritesOutbox      bool
	WritesAuditStream bool
}

func ScriptSpecs() []ScriptSpec {
	return []ScriptSpec{{ScriptAllocate, true, false}, {ScriptRenew, false, true}, {ScriptRelease, true, false}, {ScriptTransfer, true, false}, {ScriptRecover, true, false}, {ScriptDrain, true, false}, {ScriptNodeLeaseExpire, true, false}}
}
