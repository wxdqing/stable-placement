package stableplacement

import (
	"errors"
	"strings"
)

type GrainKey string

func NewGrainKey(kind string, grainID string) (GrainKey, error) {
	if kind == "" {
		return "", errors.New("kind is empty")
	}
	if grainID == "" {
		return "", errors.New("grain id is empty")
	}
	return GrainKey(kind + "/" + grainID), nil
}

func (k GrainKey) String() string {
	return string(k)
}

type NodeIdentity string

func NewNodeIdentity(nodeType string, nodeGroup string, nodeName string) (NodeIdentity, error) {
	if nodeType == "" {
		return "", errors.New("node type is empty")
	}
	if nodeGroup == "" {
		return "", errors.New("node group is empty")
	}
	if nodeName == "" {
		return "", errors.New("node name is empty")
	}
	return NodeIdentity(nodeType + "/" + nodeGroup + "/" + nodeName), nil
}

func (i NodeIdentity) String() string {
	return string(i)
}

func (i NodeIdentity) NodeType() string {
	return i.part(0)
}

func (i NodeIdentity) NodeGroup() string {
	return i.part(1)
}

func (i NodeIdentity) NodeName() string {
	return i.part(2)
}

func (i NodeIdentity) part(index int) string {
	parts := strings.Split(string(i), "/")
	if len(parts) != 3 {
		return ""
	}
	return parts[index]
}
