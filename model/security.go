package model

const SecurityGroupCount = 32

type SecurityGroups [4]byte

func SecurityGroupsFromBytes(v []byte) SecurityGroups {
	var groups SecurityGroups
	copy(groups[:], v)
	return groups
}

func (g SecurityGroups) Bytes() []byte {
	out := make([]byte, len(g))
	copy(out, g[:])
	return out
}

func (g SecurityGroups) Has(id int) bool {
	if id < 0 || id >= SecurityGroupCount {
		return false
	}
	return g[id/8]&(1<<uint(id%8)) != 0
}

func (g SecurityGroups) With(id int) (SecurityGroups, bool) {
	if id < 0 || id >= SecurityGroupCount {
		return g, false
	}
	g[id/8] |= 1 << uint(id%8)
	return g, true
}

func (g SecurityGroups) Without(id int) (SecurityGroups, bool) {
	if id < 0 || id >= SecurityGroupCount {
		return g, false
	}
	g[id/8] &^= 1 << uint(id%8)
	return g, true
}
