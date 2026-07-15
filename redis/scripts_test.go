package redis

import (
	"io/fs"
	"strings"
	"testing"
)

func TestEmbeddedLuaFilesAndComposedScriptsAreComplete(t *testing.T) {
	wantFiles := map[string]bool{
		"lua/shared/helper.lua": true, "lua/shared/event.lua": true, "lua/shared/schedule.lua": true,
		"lua/node/register.lua": true, "lua/node/renew.lua": true, "lua/node/replace_session.lua": true,
		"lua/node/unregister.lua": true, "lua/node/drain.lua": true, "lua/node/mark_invalid.lua": true,
		"lua/node/restore.lua": true, "lua/lease/expire_node.lua": true,
		"lua/placement/lookup.lua": true, "lua/placement/allocate.lua": true,
		"lua/placement/resolve_route.lua": true, "lua/placement/renew.lua": true,
		"lua/placement/mutate.lua": true, "lua/stream/trim.lua": true,
		"lua/stream/replace_consumer.lua": true, "lua/stream/close_idle_group.lua": true,
	}
	gotFiles := make(map[string]bool, len(wantFiles))
	if err := fs.WalkDir(luaFiles, "lua", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".lua") {
			t.Fatalf("unexpected embedded file %q", path)
		}
		content, readErr := luaFiles.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.TrimSpace(string(content)) == "" {
			t.Fatalf("embedded Lua file %q is empty", path)
		}
		gotFiles[path] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(gotFiles) != len(wantFiles) {
		t.Fatalf("embedded Lua file count = %d, want %d: %v", len(gotFiles), len(wantFiles), gotFiles)
	}
	for path := range wantFiles {
		if !gotFiles[path] {
			t.Fatalf("embedded Lua file %q is missing", path)
		}
	}

	composed := map[string]string{
		"register node": registerNodeLua, "renew node": renewNodeLua,
		"replace node session": replaceNodeSessionLua, "expire node lease": expireNodeLeaseLua,
		"unregister node": unregisterNodeLua, "drain node": drainNodeLua,
		"mark invalid": markInvalidLua, "restore node": restoreNodeLua,
		"lookup": lookupLua, "allocate": allocateLua, "resolve route": resolveRouteLua,
		"renew placement": renewPlacementLua, "mutate placement": mutationLua,
		"trim stream": trimStreamLua, "replace consumer": replaceConsumerLua,
		"close idle group": closeConsumerGroupIfIdleLua,
	}
	for name, script := range composed {
		if strings.TrimSpace(script) == "" {
			t.Fatalf("composed Lua script %q is empty", name)
		}
	}
}

func TestScriptSpecsSeparateRenewAuditFromCacheInvalidationOutbox(t *testing.T) {
	for _, spec := range ScriptSpecs() {
		if spec.Name == ScriptRenew {
			if spec.WritesOutbox {
				t.Fatal("renew must not write cache invalidation outbox")
			}
			if !spec.WritesAuditStream {
				t.Fatal("renew should write audit stream when audit is enabled")
			}
			continue
		}
		if !spec.WritesOutbox {
			t.Fatalf("%s must write cache invalidation outbox", spec.Name)
		}
	}
}
