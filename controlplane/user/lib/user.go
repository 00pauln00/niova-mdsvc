package userlib

import (
	"encoding/xml"
	"fmt"
	"strings"
)

// API operation constants
const (
	PutUserAPI   = "PutUser"
	GetUserAPI   = "GetUser"
	AdminUserAPI = "CreateAdminUser"
	// InitAdminUserAPI = "InitAdminUser"
)

// User constants
const (
	AdminUsername = "admin"
)

// Status is a type alias for user status values.
type Status uint8

// User status constants
const (
	StatusActive Status = iota
	StatusInactive
	StatusSuspended
)

// String returns the string representation of Status
func (s Status) String() string {
	switch s {
	case StatusActive:
		return "active"
	case StatusInactive:
		return "inactive"
	case StatusSuspended:
		return "suspended"
	default:
		return "unknown"
	}
}

// Capability represents service-level permissions for a user
type Capability struct {
	Service string `xml:"Service" json:"service"` // "nisd" or "cp"
	Perm    uint32 `xml:"Perm" json:"perm"`       // permission
}

// UserReq represents the request struct for PutUser API
type UserReq struct {
	XMLName      xml.Name     `xml:"UserReq"`
	Username     string       `xml:"Username" json:"username"`
	UserID       string       `xml:"UserID" json:"userID"`                                 // UUID string, generated server-side for create
	NewSecretKey string       `xml:"NewSecretKey,omitempty" json:"newSecretKey,omitempty"` // For updating secret key (admin only)
	Capabilities []Capability `xml:"Capabilities" json:"capabilities"`
	IsUpdate     bool         `xml:"IsUpdate,omitempty" json:"isUpdate,omitempty"` // true for update, false for create
	IsAdmin      bool         `xml:"IsAdmin,omitempty" json:"isAdmin,omitempty"`
}

// User represents the internal user entity stored in RocksDB
type User struct {
	Username     string       `xml:"Username" json:"username"`
	UserID       string       `xml:"UserID" json:"userID"`
	SecretKey    string       `xml:"SecretKey" json:"secretKey"`
	Capabilities []Capability `xml:"Capabilities" json:"capabilities"`
	Status       Status       `xml:"Status" json:"status"`
	IsAdmin      bool         `xml:"IsAdmin" json:"isAdmin"`
}

// UserResp is the response struct for PutUser and GetUser APIs
type UserResp struct {
	XMLName      xml.Name     `xml:"UserResp"`
	Username     string       `xml:"Username" json:"username"`
	UserID       string       `xml:"UserID" json:"userID"`
	SecretKey    string       `xml:"SecretKey" json:"secretKey"`
	Capabilities []Capability `xml:"Capabilities" json:"capabilities"`
	Status       Status       `xml:"Status" json:"status"`
	Error        string       `xml:"Error" json:"error"`
	Success      bool         `xml:"Success" json:"success"`
}

// GetReq represents the request struct for GetUser API
type GetReq struct {
	UserID string `xml:"UserID" json:"userID"` // UUID string, empty means get all users
}

// Validate validates UserReq structure
func (u *UserReq) Validate() error {
	if strings.TrimSpace(u.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	return nil
}

// Init initializes a new User with default values
func (u *User) Init(req *UserReq) error {
	u.Username = req.Username
	u.UserID = req.UserID
	u.Capabilities = req.Capabilities
	u.Status = StatusActive
	u.IsAdmin = req.IsAdmin
	return nil
}
