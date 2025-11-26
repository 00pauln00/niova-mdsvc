package srvctlplanefuncs

import (
	cpLib "github.com/00pauln00/niova-mdsvc/controlplane/ctlplanefuncs/lib"
	cbtree "github.com/tidwall/btree"
)

// Nisds is a Counted B-tree containing pointers to Nisd objects, ordered by Nisd.ID.
// Entity maps a single entity to the set of Nisd objects associated with it.
type Entities struct {
	ID    string
	Nisds cbtree.BTree
}

// FailureDomain groups entities under a specific failure-isolation class.
// Type defines the domain category (pdu, rack, hv etc.).
// Tree stores entities ordered by their Entity.ID using a Counted B-tree.
type FailureDomain struct {
	Type uint8
	Tree cbtree.BTree
}

// stores the Hierarchy Information
// Index 0 - PDU
// Index 1 - Rack
// Index 2 - HyperVisor
// Index 3 - Device
type Hierarchy struct {
	FD []FailureDomain
}

// Initialize the Hierarchy Struct
func (hr *Hierarchy) Init() error {
	return nil
}

// Add NISD and the corresponding parent entities to the Hierarchy
func (hr *Hierarchy) AddNisd(nisd *cpLib.Nisd) error {
	return nil
}

// Delete NISD and the corresponding parent entities from the Hierarchy
func (hr *Hierarchy) DeleteNisd(nisd *cpLib.Nisd) error {
	return nil
}

// Get the Failure Domain Based on the Nisd Count
func (hr *Hierarchy) GetFDLevel(fltTlrnc uint8) int {
	return 0
}

// Get the total number of elements in a Failure Domain.
func (hr *Hierarchy) GetEntityLen(entity int) int {
	return 0
}

// Pick a  NISD using the hash from a specific failure domain.
func (hr *Hierarchy) PickNISD(fd uint8, hash uint64) (*cpLib.Nisd, error) {
	return nil, nil
}
