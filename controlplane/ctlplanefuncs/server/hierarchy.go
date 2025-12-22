package srvctlplanefuncs

import (
	"errors"
	"fmt"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	ctlplfl "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	"github.com/tidwall/btree"
)

// Nisds is a Counted B-tree containing pointers to Nisd objects, ordered by Nisd.ID.
// Entity maps a single entity to the set of Nisd objects associated with it.
// Here the Entity could be a PDU/Rack/HV/Device
// The Nisds Tree holds all the nisd ptr associated with the Entity.
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
		log.Tracef("deleting entity: %s, no nisd's available: ", e.ID)
	}
}

// Add NISD and the corresponding parent entities to the Hierarchy
func (hr *Hierarchy) AddNisd(n *cpLib.Nisd) error {
	if n.AvailableSize < cpLib.CHUNK_SIZE {
		log.Debugf("skipping nisd addition %s, as the available size %f GB < 8GB", n.ID, BytesToGB(n.AvailableSize))
	}
	if len(n.FailureDomain) != cpLib.MAX_FD {
		err := fmt.Errorf("failed to add nisd: not enough failure domain info")
		log.Error(err)
		return err
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
		log.Tracef("deleting nisd %s, from fd %d", n.ID, i)
		e.Nisds.Delete(n)
		fd.deleteEmptyEntity(id)
	}
}

// Get the Failure Domain Based on the Nisd Count
func (hr *Hierarchy) GetFDLevel(ft int) (int, error) {
	for i := cpLib.PDU_IDX; i <= cpLib.DEVICE_IDX; i++ {
		if ft <= hr.FD[i].Tree.Len() {
			return i, nil
		}
	}
	return -1, fmt.Errorf("failed to allocate vdev as the fault tolerance level %d cannot be met", ft)
}

// Get the total number of elements in a Failure Domain.
func (hr *Hierarchy) GetEntityLen(tierIDX int) int {
	if tierIDX >= len(hr.FD) {
		return 0
	}
	return hr.FD[tierIDX].Tree.Len()
}

func (hr *Hierarchy) LookupNAddNisd(nisd *ctlplfl.Nisd, nisdMap *btree.Map[string, *ctlplfl.NisdCopy]) *ctlplfl.NisdCopy {
	if nisdPtr, ok := nisdMap.Get(nisd.ID); ok {
		return nisdPtr
	}

	copy := &ctlplfl.NisdCopy{
		AvailableSize: nisd.AvailableSize,
		Ptr:           nisd,
	}

	nisdMap.Set(nisd.ID, copy)

	log.Debugf(
		"added nisd copy to temp nisd map: id=%s, available_size=%d",
		nisd.ID,
		nisd.AvailableSize,
	)

	return copy
}

// Pick a  NISD using the hash from a specific failure domain.
func (hr *Hierarchy) PickNISD(fd int, entityIDX int, hash uint64, picked map[string]struct{}, nisdMap *btree.Map[string, *ctlplfl.NisdCopy]) (*cpLib.NisdCopy, error) {
	if int(fd) >= len(hr.FD) {
		err := fmt.Errorf("invalid failure domain: %d", fd)
		log.Error("PickNISD():", err)
		return nil, err
	}

	fdRef := hr.FD[fd]

	ent, ok := fdRef.Tree.GetAt(entityIDX)
	if !ok {
		err := fmt.Errorf("failed to fetch entity tree from idx: %d, fd: %d", entityIDX, fd)
		log.Error("GetAt(): ", err)
		return nil, err
	}

	// select NISD inside entity
	size := ent.Nisds.Len()
	if size <= 0 {
		log.Errorf("failed to get index for hash: %d", hash)
		return nil, fmt.Errorf("invalid size: %d", size)
	}
	idx := int(hash % uint64(size))

	for i := 0; i < size; i++ {
		nisd, ok := ent.Nisds.GetAt(idx)
		if !ok {
			log.Error("failed to get nisd from tree at idx: ", idx)
			return nil, errors.New("selection failure")
		}

		// update the map details here
		nCopy := HR.LookupNAddNisd(nisd, nisdMap)

		// check if the nisd can be picked or not by checking the nisd available space from the nisd map,
		// if the space is available then pick the nisd
		if nCopy.AvailableSize >= ctlplfl.CHUNK_SIZE {
			// TODO: move this to a separate filtering method
			if _, exists := picked[nCopy.Ptr.ID]; !exists {

				// decrement the available size value only after the chunk is picked
				nCopy.AvailableSize -= ctlplfl.CHUNK_SIZE
				// commit selection
				picked[nCopy.Ptr.ID] = struct{}{}
				return nCopy, nil
			}
			log.Tracef("failed to pick nisd, as it's already picked: %s", nCopy.Ptr.ID)
		}

		idx = (idx + 1) % ent.Nisds.Len()
		log.Tracef("incrementing index : ", idx)
	}
	return nil, fmt.Errorf("failed to pick nisd from the entity: %d", entityIDX)
}

func BytesToGB(b int64) float64 {
	const gb = 1024 * 1024 * 1024
	return float64(b) / float64(gb)
}

func (hr *Hierarchy) Dump() {
	if hr.FD == nil {
		log.Errorf("hierarchy Structure not initialized")
		return
	}

	for i := range hr.FD {
		fd := &hr.FD[i]
		log.Debugf("FD Type: %d", fd.Type)

		fd.Tree.Scan(func(ent *Entities) bool {
			if ent == nil {
				return true
			}

			log.Debugf("  Entity: %s", ent.ID)

			ent.Nisds.Scan(func(n *cpLib.Nisd) bool {
				if n == nil {
					return true
				}
				log.Debugf(" 	Nisd: %s: %f GB", n.ID, BytesToGB(n.AvailableSize))
				return true
			})

			return true
		})
	}
}
