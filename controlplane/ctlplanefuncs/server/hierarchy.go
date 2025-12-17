package srvctlplanefuncs

import (
	"errors"
	"fmt"

	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

// Nisds is a Counted B-tree containing pointers to Nisd objects, ordered by Nisd.ID.
// Entity maps a single entity to the set of Nisd objects associated with it.
type Entities struct {
	ID    string
	Nisds *btree.BTreeG[*cpLib.Nisd]
}

// FailureDomain groups entities under a specific failure-isolation class.
// Type defines the domain category (pdu, rack, hv etc.).
// Tree stores entities ordered by their Entity.ID using a Counted B-tree.
type FailureDomain struct {
	Type uint8
	Tree *btree.BTreeG[*Entities]
}

// stores the Hierarchy Information
// Index 0 - PDU
// Index 1 - Rack
// Index 2 - HyperVisor
// Index 3 - Device
type Hierarchy struct {
	FD []FailureDomain
	// AvailableSize uint64
	NisdMap map[string]*ctlplfl.NisdCopy
}

var HR Hierarchy

func compareEntity(a, b *Entities) bool { return a.ID < b.ID }
func compareNisd(a, b *cpLib.Nisd) bool { return a.ID < b.ID }

// Initialize the Hierarchy Struct
func (hr *Hierarchy) Init() {
	hr.FD = make([]FailureDomain, cpLib.MAX_FD)
	for i := 0; i < 4; i++ {
		hr.FD[i] = FailureDomain{
			Type: uint8(i),
			Tree: btree.NewBTreeG[*Entities](compareEntity),
		}
	}
}

func (fd *FailureDomain) getOrCreateEntity(id string) *Entities {
	e, ok := fd.Tree.Get(&Entities{ID: id})
	if ok {
		return e
	}
	n := Entities{
		ID:    id,
		Nisds: btree.NewBTreeG[*cpLib.Nisd](compareNisd),
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
	// if no elements are present in the tree, return a error
	if size <= 0 {
		return 0, errors.New("invalid size")
	}
	// if only one element is present in the tree, return 0th idx
	if size == 1 {
		return 0, nil
	}
	return int(hash % uint64(size)), nil
}

// Add NISD and the corresponding parent entities to the Hierarchy
func (hr *Hierarchy) AddNisd(n *cpLib.Nisd) error {
	if n.AvailableSize < cpLib.CHUNK_SIZE {
		log.Info("skipping nisd addition %s, as the available size %d < 8GV", n.ID, n.AvailableSize)
	}
	if len(n.FailureDomain) != cpLib.MAX_FD {
		log.Error("failed to add nisd: not enough failure domain info")
		return fmt.Errorf("failed to add nisd: not enough failure domain info")
	}
	for i, id := range n.FailureDomain {
		e := hr.FD[i].getOrCreateEntity(id)
		e.Nisds.Set(n)
	}
	return nil
}

// Delete NISD and the corresponding parent entities from the Hierarchy
func (hr *Hierarchy) DeleteNisd(n *cpLib.Nisd) {
	for i, id := range n.FailureDomain {
		fd := &hr.FD[i]
		e, ok := fd.Tree.Get(&Entities{ID: id})
		if !ok {
			continue
		}
		e.Nisds.Delete(n)
		fd.deleteEmptyEntity(id)
	}
}

// Get the Failure Domain Based on the Nisd Count
func (hr *Hierarchy) GetFDLevel(fltTlrnc int) (int, error) {
	for i := cpLib.PDU_IDX; i <= cpLib.DEVICE_IDX; i++ {
		if fltTlrnc <= hr.FD[i].Tree.Len() {
			return i, nil
		}
	}
	return -1, fmt.Errorf("failed to allocate vdev as the fault tolerance level %d cannot be met", fltTlrnc)
}

// Get the total number of elements in a Failure Domain.
func (hr *Hierarchy) GetEntityLen(entity int) int {
	if entity >= len(hr.FD) {
		return 0
	}
	return hr.FD[entity].Tree.Len()
}

func (hr *Hierarchy) UpdateNisdCopy(nisd *ctlplfl.Nisd) *ctlplfl.NisdCopy {
	if nisdPtr, exists := hr.NisdMap[nisd.ID]; exists {
		return nisdPtr
	}
	hr.NisdMap[nisd.ID] = &ctlplfl.NisdCopy{
		AvailableSize: (nisd.AvailableSize),
		Ptr:           nisd,
	}
	return hr.NisdMap[nisd.ID]
}

// Pick a  NISD using the hash from a specific failure domain.
func (hr *Hierarchy) PickNISD(fd int, entityIDX int, hash uint64, picked map[string]struct{}) (*cpLib.NisdCopy, error) {
	if int(fd) >= len(hr.FD) {
		log.Error("failed to get fd: ", fd)
		return nil, errors.New("invalid fd tier")
	}

	fdRef := hr.FD[fd]

	ent, ok := fdRef.Tree.GetAt(entityIDX)
	if !ok {
		log.Error("failed to get entitiy tree at idx: ", entityIDX)
		return nil, errors.New("entity missing")
	}

	// select NISD inside entity
	idx, err := GetIndex(hash, ent.Nisds.Len())
	if err != nil {
		log.Error("failed to get index for hash: ", hash)
		return nil, err
	}

	for i := 0; i < ent.Nisds.Len(); i++ {
		nisd, ok := ent.Nisds.GetAt(idx)
		if !ok {
			log.Error("failed to get nisd from tree at idx: ", idx)
			return nil, errors.New("selection failure")
		}

		// update the map details here
		nCopy := HR.UpdateNisdCopy(nisd)

		// check if the nisd can be picked or not by checking the nisd available space from the nisd map,
		// if the space is available then pick the nisd
		if nCopy.AvailableSize >= ctlplfl.CHUNK_SIZE {
			nCopy.AvailableSize -= ctlplfl.CHUNK_SIZE

			// TODO: move this to a separate filtering method
			if _, exists := picked[nCopy.Ptr.ID]; !exists {
				// commit selection
				picked[nCopy.Ptr.ID] = struct{}{}
				return nCopy, nil
			}
		}

		idx = (idx + 1) % ent.Nisds.Len()
		log.Info("index : ", idx)
	}
	return nil, fmt.Errorf("failed to pick nisd from the entity: %d", entityIDX)
}
func BytesToGB(b int64) float64 {
	const gb = 1024 * 1024 * 1024
	return float64(b) / float64(gb)
}

func (hr *Hierarchy) Dump() {
	if hr == nil {
		return
	}
	if hr.FD == nil {
		return
	}

	for i := range hr.FD {
		fd := &hr.FD[i]
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
				log.Infof("    Nisd: %s: %f GB\n", n.ID, BytesToGB(n.AvailableSize))
				return true
			})

			return true
		})
	}
}
