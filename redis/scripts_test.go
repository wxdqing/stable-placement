package redis

import "testing"

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
