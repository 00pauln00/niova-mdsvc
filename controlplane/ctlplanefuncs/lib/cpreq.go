package libctlplanefuncs

// CPStatus distinguishes error categories in control plane responses.
type CPStatus int

const (
	// StatusOK indicates a successful operation.
	StatusOK CPStatus = iota
	// StatusAuthError indicates token invalid/expired or RBAC/ABAC denied.
	StatusAuthError
	// StatusFuncError indicates a business logic error in the handler.
	StatusFuncError
	// StatusInternal indicates a transport/encoding/unexpected error.
	StatusInternal
)

// Pagination holds cursor-based pagination parameters for read requests.
type Pagination struct {
	SeqNo      uint64 // Seq number of the last read record
	Consistent bool   // If true, the read is consistent with the last read
}

// CPReq is the unified request envelope for all control plane operations.
// It separates auth, pagination, and function-specific payload.
type CPReq struct {
	Token   string      // Auth JWT token
	Page    Pagination  // Range read pagination params for reads
	Payload interface{} // Function-specific request struct (Nisd, Rack, GetReq, etc.)
}

// CPResp is the unified response envelope for all control plane operations.
type CPResp struct {
	Status   CPStatus    // Error category
	ErrorMsg string      // Human-readable error (empty on success)
	Page     Pagination  // Range read pagination details
	Payload  interface{} // Function-specific response
}
