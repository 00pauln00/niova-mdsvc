package userserver

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
	pmCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	pumiceFunc "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

var columnFamily string = "PMDBTS_CF"

// Default admin secret key - used for initial admin user creation
// This should be changed after first login via update request
const defaultAdminSecretKey = "niova-admin-secret-key"

// Error message returned by PMDB when key is not found
const errKeyNotFoundMsg = "Failed to lookup for key"

// isKeyNotFoundError checks if the error indicates that the key was not found in PMDB
func isKeyNotFoundError(err error) bool {
	return err != nil && err.Error() == errKeyNotFoundMsg
}

//
// var columnFamily string = "AUTH_CF"

// checkUserExistsByID checks if a user with the given UserID (UUID string) already exists
func checkUserExistsByID(userID string, callbackArgs *pumiceserver.PmdbCbArgs) (*userlib.User, error) {
	if userID == "" {
		return nil, nil
	}

	prefixKey := fmt.Sprintf("%s/%s", userKeyPrefix, userID)
	readResult, err := callbackArgs.PmdbRangeRead(pumiceserver.RangeReadArgs{
		ColFamily:  columnFamily,
		Key:        prefixKey,
		Prefix:     prefixKey,
		BufSize:    callbackArgs.ReplySize,
		SeqNum:     0,
		Consistent: false,
	})

	if isKeyNotFoundError(err) {
		// User doesn't exist
		log.Infof("Key [%v] not found\n", prefixKey)
		return nil, nil
	} else if err != nil {
		// Actual error
		return nil, fmt.Errorf("range read failure: %w", err)
	}

	existing := DeserializeSingleUser(readResult.ResultMap, userID)
	return existing, nil
}

// GetAllUsers retrieves all users from RocksDB without encoding.
// This is useful for internal operations that don't need GOB encoding overhead.
func GetAllUsers(callbackArgs *pumiceserver.PmdbCbArgs) ([]userlib.User, error) {
	prefixKey := userKeyPrefix
	allResults := make(map[string][]byte)
	var lastKey string

	for {
		rangeKey := prefixKey
		if lastKey != "" {
			rangeKey = lastKey
		}

		readResult, err := callbackArgs.PmdbRangeRead(pumiceserver.RangeReadArgs{
			ColFamily:  columnFamily,
			Key:        rangeKey,
			Prefix:     prefixKey,
			BufSize:    callbackArgs.ReplySize,
			SeqNum:     0,
			Consistent: false,
		})

		if err != nil {
			if isKeyNotFoundError(err) {
				log.Infof("Range Key:[%v], Prefix Key:[%v] not found\n", rangeKey, prefixKey)
				// No keys found with this prefix - return empty list
				break
			}
			return nil, fmt.Errorf("range read failure: %w", err)
		}

		for k, v := range readResult.ResultMap {
			allResults[k] = v
		}

		if readResult.LastKey == "" {
			break
		}
		lastKey = readResult.LastKey
	}

	users := DeserializeUsers(allResults)
	return users, nil
}

// handleCreateUser handles user creation logic
// Generates UUID for UserID (if not provided) and returns the plain text secret key for response
func handleCreateUser(req *userlib.UserReq, user *userlib.User) (string, error) {
	// Generate UUID for UserID only if not already provided (e.g., admin initialization)
	if req.UserID == "" {
		newUUID, err := uuid.NewV7()
		if err != nil {
			return "", fmt.Errorf("failed to generate UUID: %w", err)
		}
		req.UserID = newUUID.String()
	}

	// Initialize user from request
	if err := user.Init(req); err != nil {
		return "", fmt.Errorf("user initialization failed: %w", err)
	}

	// Determine the secret key to use
	var plainSecretKey string
	if req.IsAdmin {
		// Use fixed default secret key for admin user
		// This should be changed after first login via update request
		plainSecretKey = defaultAdminSecretKey
	} else {
		// Generate random secret key for regular users
		var err error
		plainSecretKey, err = userlib.GenerateSecretKey()
		if err != nil {
			return "", fmt.Errorf("failed to generate secret key: %w", err)
		}
	}

	// Encrypt the secret key before storing
	encryptedSecretKey, err := userlib.EncryptSecretKey(plainSecretKey)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt secret key: %w", err)
	}

	// Store encrypted version in user (for RocksDB storage)
	user.SecretKey = encryptedSecretKey

	// Return plain text for response
	return plainSecretKey, nil
}

// handleUpdateUser handles user update logic (username/capabilities/secretKey changes)
func handleUpdateUser(req *userlib.UserReq, existing *userlib.User) (string, error) {
	var updatedSecretKey string

	// Apply username change if provided
	if req.Username != "" && req.Username != existing.Username {
		existing.Username = req.Username
	}

	// Update capabilities if provided
	if len(req.Capabilities) > 0 {
		existing.Capabilities = req.Capabilities
	}

	// Update secret key if provided (admin only)
	if req.NewSecretKey != "" {
		if !existing.IsAdmin {
			return "", fmt.Errorf("only admin users can update their secret key")
		}

		// Encrypt the new secret key before storing
		encryptedSecretKey, err := userlib.EncryptSecretKey(req.NewSecretKey)
		if err != nil {
			return "", fmt.Errorf("failed to encrypt new secret key: %w", err)
		}
		existing.SecretKey = encryptedSecretKey
		updatedSecretKey = req.NewSecretKey // Return plain text for response
	}

	return updatedSecretKey, nil
}

// PutUser creates or updates a user's authentication credentials.
// For create operations, it generates a new UUID and secret key.
// For update operations, it updates username/capabilities but keeps the existing secret key.
// UserID (UUID) is always generated server-side for create operations.
func PutUser(args ...interface{}) (interface{}, error) {
	req := args[0].(userlib.UserReq)
	callbackArgs := args[1].(*pumiceserver.PmdbCbArgs)

	var err error
	var isCreate bool
	user := &userlib.User{}

	allUsers, err := GetAllUsers(callbackArgs)
	if err != nil {
		log.Errorf("Failed to get all users for uniqueness check: %v", err)
		return returnError(fmt.Sprintf("failed to check for username uniqueness: %v", err))
	}

	if req.IsUpdate {
		// Explicit UPDATE operation
		if req.UserID == "" {
			log.Error("UserID is required for update operation")
			return returnError("userID is required for update operation")
		}

		var existing *userlib.User
		for i := range allUsers {
			if allUsers[i].UserID == req.UserID {
				existing = &allUsers[i]
				break
			}
		}

		if existing == nil {
			log.Error("Cannot update: user not found: ", req.UserID)
			return returnError(fmt.Sprintf("user not found: %s", req.UserID))
		}

		if req.Username != "" && req.Username != existing.Username {
			for _, u := range allUsers {
				if u.Username == req.Username {
					log.Errorf("Username '%s' already exists.", req.Username)
					return returnError(fmt.Sprintf("username '%s' already exists", req.Username))
				}
			}
		}

		isCreate = false
		user = existing
	} else {
		// CREATE operation (IsUpdate == false)
		if strings.TrimSpace(req.Username) == "" {
			log.Error("Username cannot be empty for create operation")
			return returnError("username cannot be empty")
		}

		for _, u := range allUsers {
			if u.Username == req.Username {
				log.Errorf("Username '%s' already exists.", req.Username)
				return returnError(fmt.Sprintf("username '%s' already exists", req.Username))
			}
		}
		isCreate = true
	}
	// Handle create or update
	var plainSecretKey string
	if isCreate {
		plainSecretKey, err = handleCreateUser(&req, user)
		if err != nil {
			log.Error("Create operation failed: ", err)
			return returnError(err.Error())
		}
	} else {
		plainSecretKey, err = handleUpdateUser(&req, user)
		if err != nil {
			log.Error("Update operation failed: ", err)
			return returnError(err.Error())
		}
	}

	// Serialize to commit changes (user.SecretKey is encrypted)
	commitChanges := SerializeUser(user)

	// Prepare response (return plain text secret key if created or updated)
	authResponse := &userlib.UserResp{
		UserID:       user.UserID,
		Username:     user.Username,
		Status:       user.Status,
		Success:      true,
		Capabilities: user.Capabilities,
	}
	if plainSecretKey != "" {
		// Return plain text secret key in response (for create or secret key update)
		authResponse.SecretKey = plainSecretKey
	}

	encodedResponse, err := pmCommon.Encoder(pmCommon.GOB, authResponse)
	if err != nil {
		log.Error("Failed to encode response: ", err)
		return returnError(fmt.Sprintf("failed to encode response: %v", err))
	}

	funcIntermediate := pumiceFunc.FuncIntrm{
		Changes:  commitChanges,
		Response: encodedResponse,
	}

	encodedIntermediate, err := pmCommon.Encoder(pmCommon.GOB, funcIntermediate)
	if err != nil {
		log.Error("Failed to encode intermediate: ", err)
		return returnError(fmt.Sprintf("failed to encode intermediate: %v", err))
	}

	return encodedIntermediate, nil
}

// GetUser retrieves user authentication information from RocksDB.
// It supports querying by UserID (UUID string) or getting all users.
func GetUser(args ...interface{}) (interface{}, error) {
	callbackArgs := args[0].(*pumiceserver.PmdbCbArgs)
	req := args[1].(userlib.GetReq)

	// Build the key for range read
	prefixKey := userKeyPrefix
	if req.UserID != "" {
		// Query specific user by ID
		prefixKey = fmt.Sprintf("%s/%s", userKeyPrefix, req.UserID)
	}
	// Perform read from RocksDB
	readResult, err := callbackArgs.PmdbRangeRead(pumiceserver.RangeReadArgs{
		ColFamily:  columnFamily,
		Key:        prefixKey,
		Prefix:     prefixKey,
		BufSize:    callbackArgs.ReplySize,
		SeqNum:     0,
		Consistent: false,
	})

	var users []userlib.User
	if err != nil {
		if isKeyNotFoundError(err) {
			// Key/prefix not found treat as empty result (no users)
			log.Infof("Key [%v] not found\n", prefixKey)
			users = []userlib.User{}
		} else {
			log.Error("Range read failure: ", err.Error())
			return returnError(fmt.Sprintf("range read failure: %v", err))
		}
	} else {
		// Deserialize entities
		users = DeserializeUsers(readResult.ResultMap)
	}

	// Convert to UserResp slice with decrypted secret keys
	userResps := make([]userlib.UserResp, 0, len(users))
	for _, u := range users {
		// Decrypt the secret key before returning
		decryptedKey := ""
		if u.SecretKey != "" {
			decrypted, decryptErr := userlib.DecryptSecretKey(u.SecretKey)
			if decryptErr != nil {
				log.Error("Failed to decrypt secret key for user ", u.UserID, ": ", decryptErr)
				// Continue with empty key rather than failing the entire request
			} else {
				decryptedKey = decrypted
			}
		}

		userResps = append(userResps, userlib.UserResp{
			Username:     u.Username,
			UserID:       u.UserID,
			SecretKey:    decryptedKey,
			Status:       u.Status,
			Capabilities: u.Capabilities,
			Success:      true,
		})
	}

	// Encode response
	encodedUsers, err := pmCommon.Encoder(pmCommon.GOB, userResps)
	if err != nil {
		log.Error("Failed to encode user auth info: ", err)
		return returnError(fmt.Sprintf("failed to encode user auth info: %v", err))
	}

	return encodedUsers, nil
}

/*
// InitAdminUser initializes the default admin user if it doesn't exist.
// Only runs when the node is becoming leader.
func InitAdminUser(callbackArgs *pumiceserver.PmdbCbArgs) {
	if callbackArgs.InitState != pumiceserver.INIT_BECOMING_LEADER_STATE {
		return
	}

	go createAdminUserAsync()
}

// createAdminUserAsync creates the admin user after ensuring Raft is ready.
func createAdminUserAsync() {
	// Wait for Raft to be fully operational
	delay := 5 * time.Second
	time.Sleep(delay)

	var cbArgs pumiceserver.PmdbCbArgs
	cbArgs.ReplySize = 4 * 1024 * 1024 // 4MB buffer

	allUsers, err := GetAllUsers(&cbArgs)
	if err != nil {
		log.Errorf("Failed to get all users for admin check: %v", err)
		return
	}

	// Check if admin user already exists
	for _, u := range allUsers {
		if u.Username == "admin" {
			key, _ := userlib.DecryptSecretKey(u.SecretKey)
			log.Info("Admin user already exists, skipping creation")
			printUserAdminDetails(key)
			return
		}
	}

	funcReq := pumiceFunc.FuncReq{
		Name: userlib.PutUserAPI,
		Args: userlib.UserReq{
			Username: "admin",
			UserID:   uuid.Nil.String(),
			IsAdmin:  true,
			Capabilities: []userlib.Capability{
				{Service: "cp", Perm: 07},
				{Service: "nisd", Perm: 07},
			},
		},
	}

	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(funcReq)
	if err != nil {
		log.Errorf("Failed to encode FuncReq: %v", err)
		return
	}

	reqUUID, err := uuid.NewV7()
	if err != nil {
		log.Errorf("Failed to generate UUID for rncui: %v", err)
		return
	}
	rncui := fmt.Sprintf("%s:0:0:0:0", reqUUID.String())
	resp := pumiceserver.PmdbEnqueuePutRequest(buf.Bytes(), pmCommon.FUNC_REQ, rncui, 0)

	if resp == 0 {
		// Wait a bit for Apply to complete
		time.Sleep(2 * time.Second)

		// Verify the user was created
		users, err := GetAllUsers(&cbArgs)
		if err != nil {
			log.Errorf("Failed to verify user creation: %v", err)
			return
		}

		log.Infof("After creation, found %d users", len(users))
		for _, u := range users {
			if u.Username == "admin" {
				log.Info("Admin user successfully created and verified!")
				key, _ := userlib.DecryptSecretKey(u.SecretKey)
				printUserAdminDetails(key)
				return
			}
		}
		log.Warn("Admin user not found after creation - Apply may have failed")
	} else {
		log.Errorf("PmdbEnqueuePutRequest failed with resp: %d", resp)
	}
}
*/

// applyCommitChanges applies commit changes to RocksDB using PmdbWriteKV.
// This is a helper function for Apply-only patterns.
func applyCommitChanges(changes []pumiceFunc.CommitChg, cbArgs *pumiceserver.PmdbCbArgs) error {
	for _, chg := range changes {
		log.Debugf("Applying change: %s", string(chg.Key))
		rc := cbArgs.PmdbWriteKV(columnFamily, string(chg.Key), string(chg.Value))
		if rc < 0 {
			log.Errorf("Failed to apply change for key: %s", string(chg.Key))
			return fmt.Errorf("failed to apply change for key: %s", string(chg.Key))
		}
	}
	return nil
}

// CreateAdminUser creates/bootstraps the singleton admin user.
func CreateAdminUser(args ...interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("missing arguments: expected UserReq and PmdbCbArgs")
	}

	req, ok1 := args[0].(userlib.UserReq)
	cbArgs, ok2 := args[1].(*pumiceserver.PmdbCbArgs)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("type assertion failed: invalid argument types")
	}

	username := strings.TrimSpace(req.Username)
	if username == "" {
		return returnApplyError("username cannot be empty")
	}
	if username != userlib.AdminUsername {
		return returnApplyError(fmt.Sprintf("unauthorized: username must be %q", userlib.AdminUsername))
	}

	adminID := uuid.Nil.String()
	secretKey := req.NewSecretKey
	if secretKey == "" {
		secretKey = defaultAdminSecretKey
	}

	capabilities := req.Capabilities
	if len(capabilities) == 0 {
		capabilities = []userlib.Capability{
			{Service: "cp", Perm: 07},
			{Service: "nisd", Perm: 07},
		}
	}

	// Check for Existence
	existing, err := checkUserExistsByID(adminID, cbArgs)
	if err != nil {
		return nil, fmt.Errorf("datastore lookup failed: %w", err)
	}

	if existing != nil {
		log.Infof("Admin user already exists (ID: %s). Returning existing info.", adminID)

		decryptedKey, err := userlib.DecryptSecretKey(existing.SecretKey)
		if err != nil {
			log.Warnf("Could not decrypt existing admin key: %v", err)
		}

		return pmCommon.Encoder(pmCommon.GOB, userlib.UserResp{
			UserID:       existing.UserID,
			Username:     existing.Username,
			SecretKey:    decryptedKey,
			Capabilities: existing.Capabilities,
			Status:       existing.Status,
			Success:      true,
			Error:        "admin user already exists",
		})
	}

	// Creation & Persistence
	encryptedSecretKey, err := userlib.EncryptSecretKey(secretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt secret key: %w", err)
	}

	newUser := &userlib.User{
		UserID:       adminID,
		Username:     username,
		SecretKey:    encryptedSecretKey,
		IsAdmin:      true,
		Status:       userlib.StatusActive,
		Capabilities: capabilities,
	}

	if err := applyCommitChanges(SerializeUser(newUser), cbArgs); err != nil {
		return nil, fmt.Errorf("failed to persist admin user: %w", err)
	}

	log.Infof("Admin user created successfully (ID: %s)", adminID)

	return pmCommon.Encoder(pmCommon.GOB, userlib.UserResp{
		UserID:       newUser.UserID,
		Username:     newUser.Username,
		SecretKey:    secretKey, // Return the raw key for the initial creation response
		Capabilities: newUser.Capabilities,
		Status:       newUser.Status,
		Success:      true,
	})
}

// returnApplyError returns an encoded error response for Apply functions
func returnApplyError(errMsg string) (interface{}, error) {
	errorResponse := &userlib.UserResp{
		Error:   errMsg,
		Success: false,
	}
	return pmCommon.Encoder(pmCommon.GOB, errorResponse)
}

func printUserAdminDetails(key string) {
	log.Infof("Default User Admin key: %v", key)
}

func returnError(errMsg string) (interface{}, error) {
	errorResponse := &userlib.UserResp{
		Error: errMsg,
	}

	encodedResponse, err := pmCommon.Encoder(pmCommon.GOB, errorResponse)
	if err != nil {
		log.Error("Failed to encode error response: ", err)
		return nil, err
	}

	funcIntermediate := pumiceFunc.FuncIntrm{
		Changes:  nil,
		Response: encodedResponse,
	}

	encodedIntermediate, err := pmCommon.Encoder(pmCommon.GOB, funcIntermediate)
	if err != nil {
		log.Error("Failed to encode error intermediate: ", err)
		return nil, err
	}

	return encodedIntermediate, nil
}
