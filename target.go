package stableplacement

import "fmt"

type KindRouteConfig struct {
	NodeType        string
	NodeGroupPrefix string
	GroupIDLabel    string
}

type NodeTarget struct {
	NodeType  string
	NodeGroup string
}

func BuildNodeGroup(kind string, labels map[string]string, routes map[string]KindRouteConfig) (NodeTarget, error) {
	if err := ValidateIdentityPart(kind, MaxIdentityPartBytes); err != nil {
		return NodeTarget{}, fmt.Errorf("%w: kind: %v", ErrPlacementConfigInvalid, err)
	}
	config, ok := routes[kind]
	if !ok {
		return NodeTarget{}, fmt.Errorf("%w: unknown kind %q", ErrPlacementConfigInvalid, kind)
	}
	if err := ValidateIdentityPart(config.NodeType, MaxIdentityPartBytes); err != nil {
		return NodeTarget{}, fmt.Errorf("%w: node type: %v", ErrPlacementConfigInvalid, err)
	}
	if err := ValidateIdentityPart(config.GroupIDLabel, MaxIdentityPartBytes); err != nil {
		return NodeTarget{}, fmt.Errorf("%w: group id label: %v", ErrPlacementConfigInvalid, err)
	}
	groupID, ok := labels[config.GroupIDLabel]
	if !ok {
		return NodeTarget{}, fmt.Errorf("%w: missing label %q", ErrPlacementConfigInvalid, config.GroupIDLabel)
	}
	if err := ValidateIdentityPart(groupID, MaxIdentityPartBytes); err != nil {
		return NodeTarget{}, fmt.Errorf("%w: group id: %v", ErrPlacementConfigInvalid, err)
	}
	nodeGroup := config.NodeGroupPrefix + groupID
	if err := ValidateIdentityPart(nodeGroup, MaxIdentityPartBytes); err != nil {
		return NodeTarget{}, fmt.Errorf("%w: node group: %v", ErrPlacementConfigInvalid, err)
	}
	return NodeTarget{NodeType: config.NodeType, NodeGroup: nodeGroup}, nil
}
