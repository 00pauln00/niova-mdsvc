package libctlplanefuncs

import "errors"

type CPErrCode string

const (
	ErrAuth     CPErrCode = "AUTH_ERROR" // token invalid/expired or RBAC denied
	ErrFunc     CPErrCode = "FUNC_ERROR" // business logic / handler error
	ErrInternal CPErrCode = "INTERNAL"   // transport/encoding/unexpected error
)

// CPError carries a human-readable error message from the server.
type CPError struct {
	Message string    `xml:"Message"`
	Code    CPErrCode `xml:"Code"`
}

// Pagination holds cursor-based pagination parameters for read requests.
// SeqNo is kept inside the continuation Token (PageToken); callers do not
// need to supply it separately.
type Pagination struct {
	SeqNo      uint64 `xml:"SeqNo"`      // Seq number of the last read record
	Consistent bool   `xml:"Consistent"` // If true, the read is consistent with the last read
	LastKey    string `xml:"LastKey"`    // The last key read (used for pagination)
	Limit      uint32 `xml:"Limit"`      // Max number of records to return
}

// CPReq is the unified request envelope for all control plane operations.
// It separates auth, pagination, and function-specific payload.
type CPReq struct {
	Token   string      `xml:"Token"`   // Auth JWT token
	Page    *Pagination `xml:"Page"`    // Pagination parameters (used only for list/read operations)
	Payload any         `xml:"Payload"` // Function-specific request struct (Nisd, Rack, GetReq, etc.)
}

// CPResp is the unified response envelope for all control plane operations.
type CPResp struct {
	Error   *CPError    `xml:"Error"`   // nil on success
	Page    *Pagination `xml:"Page"`    // Pagination metadata (returned on list operations)
	Payload any         `xml:"Payload"` // Function-specific response
}

// Err returns a Go error from the response, or nil on success.
func (r *CPResp) Err() error {
	if r.Error == nil {
		return nil
	}
	return errors.New(r.Error.Message)
}
