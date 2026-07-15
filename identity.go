package stableplacement

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

type GrainKey string

const (
	nodeTypePartIndex = iota
	nodeGroupPartIndex
	nodeNamePartIndex
	nodeIdentityPartCount
	identitySeparatorRune = '/'
	maxASCIIControlRune   = '\x1f'
	asciiDeleteRune       = '\x7f'
)

const identitySeparator = "/"

func NewGrainKey(kind string, grainID string) (GrainKey, error) {
	if err := ValidateIdentityPart(kind, MaxIdentityPartBytes); err != nil {
		return "", fmt.Errorf("kind: %w", err)
	}
	if err := ValidateIdentityPart(grainID, MaxGrainIDBytes); err != nil {
		return "", fmt.Errorf("grain id: %w", err)
	}
	return GrainKey(kind + identitySeparator + grainID), nil
}

func (k GrainKey) String() string {
	return string(k)
}

type NodeIdentity string

func NewNodeIdentity(nodeType string, nodeGroup string, nodeName string) (NodeIdentity, error) {
	if err := ValidateIdentityPart(nodeType, MaxIdentityPartBytes); err != nil {
		return "", fmt.Errorf("node type: %w", err)
	}
	if err := ValidateIdentityPart(nodeGroup, MaxIdentityPartBytes); err != nil {
		return "", fmt.Errorf("node group: %w", err)
	}
	if err := ValidateIdentityPart(nodeName, MaxIdentityPartBytes); err != nil {
		return "", fmt.Errorf("node name: %w", err)
	}
	return NodeIdentity(nodeType + identitySeparator + nodeGroup + identitySeparator + nodeName), nil
}

func ValidateIdentityPart(value string, maxBytes int) error {
	if value == "" {
		return errors.New("value is empty")
	}
	if maxBytes <= 0 || len(value) > maxBytes {
		return fmt.Errorf("value exceeds %d bytes", maxBytes)
	}
	if !utf8.ValidString(value) {
		return errors.New("value is not valid UTF-8")
	}
	if strings.TrimSpace(value) != value {
		return errors.New("value has surrounding whitespace")
	}
	for _, r := range value {
		if r == identitySeparatorRune || r <= maxASCIIControlRune || r == asciiDeleteRune {
			return errors.New("value contains a reserved character")
		}
	}
	return nil
}

func (i NodeIdentity) String() string {
	return string(i)
}

func (i NodeIdentity) NodeType() string {
	return i.part(nodeTypePartIndex)
}

func (i NodeIdentity) NodeGroup() string {
	return i.part(nodeGroupPartIndex)
}

func (i NodeIdentity) NodeName() string {
	return i.part(nodeNamePartIndex)
}

func (i NodeIdentity) part(index int) string {
	parts := strings.Split(string(i), identitySeparator)
	if len(parts) != nodeIdentityPartCount {
		return ""
	}
	return parts[index]
}
