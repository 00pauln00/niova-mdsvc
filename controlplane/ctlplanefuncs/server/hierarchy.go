package srvctlplanefuncs

import (
	"errors"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	log "github.com/sirupsen/logrus"
	cbtree "github.com/tidwall/btree"
)

// Nisds is a Counted B-tree containing pointers to Nisd objects, ordered by Nisd.ID.
// Entity maps a single entity to the set of Nisd objects associated with it.
type Entities struct {
	ID    string
	Nisds *cbtree.BTreeG[*cpLib.Nisd]
}

// FailureDomain groups entities under a specific failure-isolation class.
// Type defines the domain category (pdu, rack, hv etc.).
// Tree stores entities ordered by their Entity.ID using a Counted B-tree.
type FailureDomain struct {
	Type uint8
	Tree *cbtree.BTreeG[*Entities]
}

// stores the Hierarchy Information
// Index 0 - PDU
// Index 1 - Rack
// Index 2 - HyperVisor
// Index 3 - Device
type Hierarchy struct {
	FD []FailureDomain
}

var HR Hierarchy

func compareEntity(a, b *Entities) bool { return a.ID < b.ID }
func compareNisd(a, b *cpLib.Nisd) bool { return a.ID < b.ID }

// Initialize the Hierarchy Struct
func (hr *Hierarchy) Init() error {
	hr.FD = make([]FailureDomain, 4)
	for i := 0; i < 4; i++ {
		hr.FD[i] = FailureDomain{
			Type: uint8(i),
			Tree: cbtree.NewBTreeG[*Entities](compareEntity),
		}
	}
	return nil
}

func (fd *FailureDomain) getOrCreateEntity(id string) *Entities {
	e, ok := fd.Tree.Get(&Entities{ID: id})
	if ok {
		return e
	}
	n := Entities{
		ID:    id,
		Nisds: cbtree.NewBTreeG[*cpLib.Nisd](compareNisd),
	}
	fd.Tree.Set(&n)
	return &n
}

func (fd *FailureDomain) deleteEmptyEntity(id string) {
	e, ok := fd.Tree.Get(&Entities{ID: id})
	if !ok {
		return
	}
	if e.Nisds.Len() == 0 {
		fd.Tree.Delete(e)
	}
}

func GetIndex(hash uint64, size int) (int, error) {
	if size <= 0 {
		return 0, errors.New("invalid size")
	}
	return int(hash % uint64(size)), nil
}

// Add NISD and the corresponding parent entities to the Hierarchy
func (hr *Hierarchy) AddNisd(n *cpLib.Nisd) error {
	//Tier 0: PDU
	{
		e := hr.FD[cpLib.PDU_IDX].getOrCreateEntity(n.FailureDomain[cpLib.PDU_IDX])
		e.Nisds.Set(n)
	}

	// Tier 1: Rack
	{
		e := hr.FD[cpLib.RACK_IDX].getOrCreateEntity(n.FailureDomain[cpLib.RACK_IDX])
		e.Nisds.Set(n)
	}

	// Tier 2: Hypervisor
	{
		e := hr.FD[cpLib.HV_IDX].getOrCreateEntity(n.FailureDomain[cpLib.HV_IDX])
		e.Nisds.Set(n)
	}

	// Tier 3: Device
	{
		e := hr.FD[cpLib.DEVICE_IDX].getOrCreateEntity(n.FailureDomain[cpLib.DEVICE_IDX])
		e.Nisds.Set(n)
	}

	return nil
}

// Delete NISD and the corresponding parent entities from the Hierarchy
func (hr *Hierarchy) DeleteNisd(n *cpLib.Nisd) error {
	for i, id := range n.FailureDomain {
		fd := &hr.FD[i]
		e, ok := fd.Tree.Get(&Entities{ID: id})
		if !ok {
			continue
		}
		e.Nisds.Delete(n)
		fd.deleteEmptyEntity(id)
	}
	return nil
}

// Get the Failure Domain Based on the Nisd Count
func (hr *Hierarchy) GetFDLevel(fltTlrnc int) int {
	for i := cpLib.PDU_IDX; i <= cpLib.DEVICE_IDX; i++ {
		if fltTlrnc < hr.FD[i].Tree.Len() {
			return i
		}
	}
	return -1
}

// Get the total number of elements in a Failure Domain.
func (hr *Hierarchy) GetEntityLen(entity int) int {
	if entity >= len(hr.FD) {
		return 0
	}
	return hr.FD[entity].Tree.Len()
}

// Pick a  NISD using the hash from a specific failure domain.
func (hr *Hierarchy) PickNISD(fd int, hash uint64) (*cpLib.Nisd, error) {

	if int(fd) >= len(hr.FD) {
		return nil, errors.New("invalid fd tier")
	}

	fdRef := hr.FD[fd]

	// select entity by index
	entityIDX, err := GetIndex(hash, fdRef.Tree.Len())
	if err != nil {
		return nil, err
	}

	ent, ok := fdRef.Tree.GetAt(entityIDX)
	if !ok {
		return nil, errors.New("entity missing")
	}

	// select NISD inside entity
	idx, err := GetIndex(hash, ent.Nisds.Len())
	if err != nil {
		return nil, err
	}

	nisd, ok := ent.Nisds.GetAt(idx)
	if !ok {
		return nil, errors.New("selection failure")
	}

	return nisd, nil
}

func (h *Hierarchy) Dump() {
	if h == nil {
		return
	}
	if h.FD == nil {
		return
	}

	for i := range h.FD {
		fd := &h.FD[i]
		log.Infof("FD Type: %d", fd.Type)

		fd.Tree.Scan(func(ent *Entities) bool {
			if ent == nil {
				return true
			}

			log.Infof("  Entity: %s\n", ent.ID)

			ent.Nisds.Scan(func(n *cpLib.Nisd) bool {
				if n == nil {
					return true
				}
				log.Infof("    Nisd: %s\n", n.ID)
				return true
			})

			return true
		})
	}
}
