package libctlplanefuncs

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

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
// Iterators resume from the provided `LastKey` up to a predetermined response size cache limit.
type Pagination struct {
	Consistent bool   `xml:"Consistent"` // If true, the read is consistent with the last read
	Token      string `xml:"Token"`      // Encoded LastKey and SeqNo
}

type TokenData struct {
	SeqNo   uint64 `json:"SeqNo"`
	LastKey string `json:"LastKey"`
}

// GetTokenData decodes the SeqNo and LastKey from the Pagination Token.
func (p *Pagination) GetTokenData() (uint64, string) {
	if p == nil || p.Token == "" {
		return 0, ""
	}
	b, err := base64.StdEncoding.DecodeString(p.Token)
	if err != nil {
		return 0, ""
	}
	var td TokenData
	if err := json.Unmarshal(b, &td); err != nil {
		return 0, ""
	}
	return td.SeqNo, td.LastKey
}

// SetTokenData encodes SeqNo and LastKey into the Pagination Token.
func (p *Pagination) SetTokenData(seqNo uint64, lastKey string) {
	if p == nil {
		return
	}
	if lastKey == "" && seqNo == 0 {
		p.Token = ""
		return
	}
	b, _ := json.Marshal(TokenData{SeqNo: seqNo, LastKey: lastKey})
	p.Token = base64.StdEncoding.EncodeToString(b)
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
