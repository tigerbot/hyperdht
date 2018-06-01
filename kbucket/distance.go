package kbucket

// DistanceCmp represents some metric of determining which of 2 contact IDs are closer
// to a particular target. Closer should return true if a is closer to the target than b.
type DistanceCmp interface {
	Closer(a, b []byte) bool
}

// XORDistance implements DistanceCmp and is the default method for measuring distance.
type XORDistance []byte

// Closer implements DistanceCmp.Closer.
func (d XORDistance) Closer(a, b []byte) bool {
	shortest, dl, al, bl := 0, len(d), len(a), len(b)
	if dl <= al && dl <= bl {
		shortest = dl
	} else if al <= bl {
		shortest = al
	} else {
		shortest = bl
	}

	for i := range d[:shortest] {
		if xa, xb := d[i]^a[i], d[i]^b[i]; xa < xb {
			return true
		} else if xb < xa {
			return false
		}
	}

	// `a` and `b` are identical up to the shortest buffer we are dealing with, so return
	// whichever one has length closest to the length of `d`.
	if (al-dl)*(al-dl) < (bl-dl)*(bl-dl) {
		return true
	}
	return false
}

type distSorter struct {
	list []Contact
	cmp  DistanceCmp
}

func (s distSorter) Len() int           { return len(s.list) }
func (s distSorter) Swap(i, j int)      { s.list[i], s.list[j] = s.list[j], s.list[i] }
func (s distSorter) Less(i, j int) bool { return s.cmp.Closer(s.list[i].ID(), s.list[j].ID()) }
