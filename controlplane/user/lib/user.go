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
	LoginAPI     = "Login"
)

// User constants
const (
	AdminUsername   = "admin"
	DefaultUserRole = "user"
	AdminUserRole   = "admin"
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

// UserReq represents the request struct for PutUser API
type UserReq struct {
	XMLName      xml.Name `xml:"UserReq"`
	Username     string   `xml:"Username" json:"username"`
	UserID       string   `xml:"UserID" json:"userID"`                                 // UUID string, generated server-side for create
	NewSecretKey string   `xml:"NewSecretKey,omitempty" json:"newSecretKey,omitempty"` // For updating secret key (admin only)
	IsUpdate     bool     `xml:"IsUpdate,omitempty" json:"isUpdate,omitempty"`         // true for update, false for create
	IsAdmin      bool     `xml:"IsAdmin,omitempty" json:"isAdmin,omitempty"`
}

// User represents the internal user entity stored in RocksDB
type User struct {
	Username  string `xml:"Username" json:"username"`
	UserID    string `xml:"UserID" json:"userID"`
	SecretKey string `xml:"SecretKey" json:"secretKey"`
	UserRole  string `xml:"UserRole" json:"userRole"`
	Status    Status `xml:"Status" json:"status"`
	IsAdmin   bool   `xml:"IsAdmin" json:"isAdmin"`
}

// UserResp is the response struct for PutUser and GetUser APIs
type UserResp struct {
	XMLName   xml.Name `xml:"UserResp"`
	Username  string   `xml:"Username" json:"username"`
	UserID    string   `xml:"UserID" json:"userID"`
	SecretKey string   `xml:"SecretKey" json:"secretKey"`
	UserRole  string   `xml:"UserRole" json:"userRole"`
	Status    Status   `xml:"Status" json:"status"`
	Error     string   `xml:"Error" json:"error"`
	Success   bool     `xml:"Success" json:"success"`
}

// GetReq represents the request struct for GetUser API, empty means get all users
type GetReq struct {
	UserID   string `xml:"UserID" json:"userID"`
	Username string `xml:"Username" json:"username"`
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
	u.Status = StatusActive
	u.IsAdmin = req.IsAdmin
	if req.IsAdmin {
		u.UserRole = AdminUserRole
	} else {
		u.UserRole = DefaultUserRole
	}
	return nil
}

// LoginReq represents the request struct for Login API
type LoginReq struct {
	Username  string `xml:"Username" json:"username"`
	SecretKey string `xml:"SecretKey" json:"secretKey"`
}

// LoginResp represents the response struct for Login API
type LoginResp struct {
	AccessToken string `xml:"AccessToken" json:"accessToken"`
	TokenType   string `xml:"TokenType" json:"tokenType"`
	UserID      string `xml:"UserID" json:"userID"`
	Username    string `xml:"Username" json:"username"`
	UserRole    string `xml:"UserRole" json:"userRole"`
	Error       string `xml:"Error" json:"error,omitempty"`
	ExpiresIn   int64  `xml:"ExpiresIn" json:"expiresIn"`
	IsAdmin     bool   `xml:"IsAdmin" json:"isAdmin"`
	Success     bool   `xml:"Success" json:"success"`
}

// Validate Login Request
func (l *LoginReq) Validate() error {
	if strings.TrimSpace(l.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}
	if strings.TrimSpace(l.SecretKey) == "" {
		return fmt.Errorf("secretKey cannot be empty")
	}

	return nil
}
