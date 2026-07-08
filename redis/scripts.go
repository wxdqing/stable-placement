package redis

const (
	ScriptAllocate = "allocate"
	ScriptRenew    = "renew"
	ScriptRelease  = "release"
	ScriptTransfer = "transfer"
	ScriptRecover  = "recover"
	ScriptExpire   = "expire"
	ScriptDrain    = "drain"
)

type ScriptSpec struct {
	Name              string
	WritesOutbox      bool
	WritesAuditStream bool
}

func ScriptSpecs() []ScriptSpec {
	return []ScriptSpec{
		{Name: ScriptAllocate, WritesOutbox: true},
		{Name: ScriptRenew, WritesAuditStream: true},
		{Name: ScriptRelease, WritesOutbox: true},
		{Name: ScriptTransfer, WritesOutbox: true},
		{Name: ScriptRecover, WritesOutbox: true},
		{Name: ScriptExpire, WritesOutbox: true},
		{Name: ScriptDrain, WritesOutbox: true},
	}
}
