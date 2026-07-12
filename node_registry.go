package stableplacement

import "context"

type NodeRegistry interface {
	RegisterNode(ctx context.Context, node Node) error
	RenewNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error
	ExpireNodeLeases(ctx context.Context, nodeType string, nodeGroup string, limit int64) (int, error)
	UnregisterNode(ctx context.Context, nodeIdentity string, nodeSessionID string) error
	ReplaceNodeSession(ctx context.Context, node Node) (*Node, error)
	FindNodes(ctx context.Context, nodeType string, nodeGroup string) ([]Node, error)
	DrainNode(ctx context.Context, nodeIdentity string) error
	CompleteDrain(ctx context.Context, nodeIdentity string, nodeSessionID string) error
	MarkNodeInvalid(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error
	RestoreNode(ctx context.Context, nodeType string, nodeGroup string, nodeName string) error
	ListInvalidNodes(ctx context.Context, nodeType string, nodeGroup string) ([]string, error)
}
