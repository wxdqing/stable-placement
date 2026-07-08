package memory

type InvalidNodeGroup struct {
	values map[string]map[string]struct{}
}

func NewInvalidNodeGroup() *InvalidNodeGroup {
	return &InvalidNodeGroup{values: make(map[string]map[string]struct{})}
}
