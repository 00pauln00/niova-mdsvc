package authlib

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// API operation constants
const (
	PutSecretKeyAPI = "PutSecretKey"
	GetSecretKeyAPI = "GetSecretKey"
)

// Status is a type alias for user status strings.
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

// UserAuth represents authentication entity for a user.
type UserAuth struct {
	XMLName   xml.Name  `xml:"UserAuth"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	Username  string    `xml:"Username" json:"username"`
	UserID    string    `xml:"UserID" json:"userID"`
	SecretKey string    `xml:"SecretKey" json:"secretKey"`
	Status    Status    `xml:"Status" json:"status"`
	IsUpdate  bool      `xml:"IsUpdate,omitempty" json:"isUpdate,omitempty"` // true for update, false for create
}

// AuthResponse is returend when creating user authentication
type AuthResponse struct {
	XMLName   xml.Name `xml:"AuthResponse"`
	SecretKey string   `xml:"SecretKey" json:"secretKey"`
	UserID    string   `xml:"UserID" json:"userID"`
	Username  string   `xml:"Username" json:"username"`
	Error     string   `xml:"Error" json:"error"`
	Status    Status   `xml:"Status" json:"status"`
	Success   bool     `xml:"Success" json:"success"`
}

// GetAuthReq represents a request to retrive user authentication info
type GetAuthReq struct {
	UserID   string `json:"userID,omitempty"`
	UserName string `json:"userName,omitempty"`
}

// Validate validates UserAuth structure
func (u *UserAuth) Validate() error {
	if strings.TrimSpace(u.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	if u.UserID != "" {
		if _, err := uuid.Parse(u.UserID); err != nil {
			return fmt.Errorf("invalid userID format, must be valid UUID")
		}
	}

	if u.Status != StatusActive && u.Status != StatusInactive && u.Status != StatusSuspended {
		return fmt.Errorf("invalid status, must be active, inactive, or suspended")
	}
	return nil
}

// Validate validates GetAuthRequest structure
func (r *GetAuthReq) Validate() error {
	if r.UserID != "" {
		if _, err := uuid.Parse(r.UserID); err != nil {
			return fmt.Errorf("invalid userID format, must be valid UUID")
		}
	}
	return nil
}

// Init initializes a new UserAuth with default values
func (u *UserAuth) Init() error {
	if u.UserID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		u.UserID = id.String()
	}

	u.CreatedAt = time.Now()
	return nil
}
