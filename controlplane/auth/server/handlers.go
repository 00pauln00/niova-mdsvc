package authserver

import (
	"fmt"
	"strings"
	"time"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/lib"
	pmCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	pumiceFunc "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
)

var columnFamily string = "AUTH_CF"

// checkUserExistsByID checks if a user with the given UserID already exists
func checkUserExistsByID(userID string, callbackArgs *pumiceserver.PmdbCbArgs) (*auth.UserAuth, error) {
	if userID == "" {
		return nil, nil
	}

	prefixKey := fmt.Sprintf("%s/%s/%s", authKey, authUsersKey, userID)
	readResult, err := callbackArgs.PmdbRangeRead(pumiceserver.RangeReadArgs{
		ColFamily:  columnFamily,
		Key:        prefixKey,
		Prefix:     prefixKey,
		BufSize:    callbackArgs.ReplySize,
		SeqNum:     0,
		Consistent: false,
	})

	if err != nil && err.Error() == "Failed to lookup for key" {
		// User doesn't exist
		return nil, nil
	} else if err != nil {
		// Actual error
		return nil, fmt.Errorf("range read failure: %w", err)
	}

	existing := DeserializeSingleEntity(readResult.ResultMap, userID)
	return existing, nil
}

// checkUsernameAvailable checks if a username is available (not taken by another user)
func checkUsernameAvailable(username string, callbackArgs *pumiceserver.PmdbCbArgs) error {
	indexKey := fmt.Sprintf("%s/%s/%s", authKey, authUsernamesKey, username)
	readIndex, err := callbackArgs.PmdbRangeRead(pumiceserver.RangeReadArgs{
		ColFamily:  columnFamily,
		Key:        indexKey,
		Prefix:     indexKey,
		BufSize:    callbackArgs.ReplySize,
		SeqNum:     0,
		Consistent: false,
	})

	// "Failed to lookup for key" means the username doesn't exist (available)
	if err != nil && err.Error() != "Failed to lookup for key" {
		return fmt.Errorf("index read failure: %w", err)
	}

	if readIndex != nil && len(readIndex.ResultMap) > 0 {
		return fmt.Errorf("username %s is already taken", username)
	}

	return nil
}

// checkSecretKeyAvailable checks if a secret key is available (not used by another user)
func checkSecretKeyAvailable(secretKey string, callbackArgs *pumiceserver.PmdbCbArgs) error {
	indexKey := fmt.Sprintf("%s/%s/%s", authKey, authSecretKeysKey, secretKey)
	readIndex, err := callbackArgs.PmdbRangeRead(pumiceserver.RangeReadArgs{
		ColFamily:  columnFamily,
		Key:        indexKey,
		Prefix:     indexKey,
		BufSize:    callbackArgs.ReplySize,
		SeqNum:     0,
		Consistent: false,
	})

	// "Failed to lookup for key" means the secret key doesn't exist (available)
	if err != nil && err.Error() != "Failed to lookup for key" {
		return fmt.Errorf("secret key index read failure: %w", err)
	}

	if readIndex != nil && len(readIndex.ResultMap) > 0 {
		return fmt.Errorf("secret key collision detected (extremely rare)")
	}

	return nil
}

// handleCreateUser handles user creation logic
func handleCreateUser(user *auth.UserAuth, callbackArgs *pumiceserver.PmdbCbArgs) error {
	// Check username availability
	if err := checkUsernameAvailable(user.Username, callbackArgs); err != nil {
		return err
	}

	// Initialize user (generate UserID if needed, set defaults)
	if err := user.Init(); err != nil {
		return fmt.Errorf("user initialization failed: %w", err)
	}

	// Verify UserID uniqueness (for both custom and auto-generated UUIDs)
	existing, checkErr := checkUserExistsByID(user.UserID, callbackArgs)
	if checkErr != nil {
		return checkErr
	}
	if existing != nil {
		return fmt.Errorf("userID %s already exists (collision)", user.UserID)
	}

	// Generate secret key with collision detection
	// With 128-bit CSPRNG, collision is rare, but we check anyway
	const maxRetries = 3
	var secretKey string
	var keyErr error

	for i := 0; i < maxRetries; i++ {
		secretKey, keyErr = auth.GenerateSecretKey()
		if keyErr != nil {
			return fmt.Errorf("failed to generate secret key: %w", keyErr)
		}

		// Check for collision (O(1) lookup via index)
		if keyErr = checkSecretKeyAvailable(secretKey, callbackArgs); keyErr != nil {
			if i < maxRetries-1 {
				log.Warn("Secret key collision detected (retry ", i+1, "/", maxRetries, ")")
				continue // Retry with new key
			}
			return fmt.Errorf("failed to generate unique secret key after %d retries: %w", maxRetries, keyErr)
		}

		// Success - unique key generated
		break
	}

	user.SecretKey = secretKey
	return nil
}

// handleUpdateUser handles user update logic (username/status changes)
func handleUpdateUser(user *auth.UserAuth, callbackArgs *pumiceserver.PmdbCbArgs) ([]pumiceFunc.CommitChg, error) {
	// Fetch existing user
	existing, err := checkUserExistsByID(user.UserID, callbackArgs)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("user not found: %s", user.UserID)
	}

	originalUsername := existing.Username
	var deleteChgs []pumiceFunc.CommitChg

	// Apply changes
	if user.Username != "" && user.Username != existing.Username {
		// Check new username availability
		if err := checkUsernameAvailable(user.Username, callbackArgs); err != nil {
			return nil, err
		}
		existing.Username = user.Username

		// Prepare delete for old username index
		oldIndexKey := []byte(fmt.Sprintf("%s/usernames/%s", authKey, originalUsername))
		deleteChgs = append(deleteChgs, pumiceFunc.CommitChg{Key: oldIndexKey, Value: nil})
	}

	if user.Status != existing.Status {
		existing.Status = user.Status
	}

	// Update the timestamp
	existing.UpdatedAt = time.Now()

	// Validate updated user
	if err := existing.Validate(); err != nil {
		return nil, fmt.Errorf("updated user validation failed: %w", err)
	}

	// Update the user object with existing data
	*user = *existing

	return deleteChgs, nil
}

// PutSecretKey creates or updates a user's authentication credentials.
// For create operations, it always generates a new secret key.
// For update operations, it updates username/status but keeps the existing secret key (no rotation).
// Secret keys are always generated server-side for security.
func PutSecretKey(args ...interface{}) (interface{}, error) {
	user := args[0].(auth.UserAuth)
	callbackArgs := args[1].(*pumiceserver.PmdbCbArgs)

	// Determine if this is create or update based on IsUpdate flag
	var deleteChgs []pumiceFunc.CommitChg
	var err error
	var isCreate bool

	if user.IsUpdate {
		// Explicit UPDATE operation
		if user.UserID == "" {
			log.Error("UserID is required for update operation")
			return returnError("userID is required for update operation")
		}
		// Verify user exists
		existing, checkErr := checkUserExistsByID(user.UserID, callbackArgs)
		if checkErr != nil {
			log.Error("Failed to check user existence: ", checkErr)
			return returnError(fmt.Sprintf("failed to check user existence: %v", checkErr))
		}
		if existing == nil {
			log.Error("Cannot update: user not found: ", user.UserID)
			return returnError(fmt.Sprintf("user not found: %s", user.UserID))
		}
		isCreate = false
	} else {
		// CREATE operation (IsUpdate == false)
		// Validate username is provided for create
		if strings.TrimSpace(user.Username) == "" {
			log.Error("Username cannot be empty for create operation")
			return returnError("username cannot be empty")
		}

		if user.UserID != "" {
			// Custom UserID provided - check for collision
			existing, checkErr := checkUserExistsByID(user.UserID, callbackArgs)
			if checkErr != nil {
				log.Error("Failed to check user existence: ", checkErr)
				return returnError(fmt.Sprintf("failed to check user existence: %v", checkErr))
			}
			if existing != nil {
				// User with this ID already exists - fail the create
				log.Error("Cannot create user: UserID already exists: ", user.UserID)
				return returnError(fmt.Sprintf("userID %s already exists", user.UserID))
			}
		}
		isCreate = true
	}

	// Handle create or update
	if isCreate {
		err = handleCreateUser(&user, callbackArgs)
		if err != nil {
			log.Error("Create operation failed: ", err)
			return returnError(err.Error())
		}
		// No deleteChgs for create operations
		deleteChgs = nil
	} else {
		deleteChgs, err = handleUpdateUser(&user, callbackArgs)
		if err != nil {
			log.Error("Update operation failed: ", err)
			return returnError(err.Error())
		}
	}

	// Serialize to commit changes
	commitChanges := SerializeAuthUser(&user)
	commitChanges = append(commitChanges, deleteChgs...)

	// Prepare response (return secret key only if created)
	authResponse := &auth.AuthResponse{
		SecretKey: "",
		UserID:    user.UserID,
		Username:  user.Username,
		Status:    user.Status,
		Success:   true,
	}
	if isCreate {
		authResponse.SecretKey = user.SecretKey
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

// GetSecretKey retrieves user authentication information from RocksDB.
// It supports querying by UserID or getting all users.
func GetSecretKey(args ...interface{}) (interface{}, error) {
	callbackArgs := args[0].(*pumiceserver.PmdbCbArgs)
	request := args[1].(auth.GetAuthReq)

	// Validate request
	if err := request.Validate(); err != nil {
		log.Error("Request validation failed: ", err)
		return returnError(fmt.Sprintf("request validation failed: %v", err))
	}

	// Build the key for range read
	prefixKey := fmt.Sprintf("%s/%s", authKey, authUsersKey)
	if request.UserID != "" {
		// Query specific user by ID
		prefixKey = fmt.Sprintf("%s/%s/%s", authKey, authUsersKey, request.UserID)
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
	if err != nil {
		log.Error("Range read failure: ", err)
		return returnError(fmt.Sprintf("range read failure: %v", err))
	}

	// Deserialize entities
	userDeserializer := UserAuthDeserializer{}
	users := DeserializeEntities[auth.UserAuth](readResult.ResultMap, userDeserializer)

	// If querying by username, filter the results
	if request.UserName != "" {
		filteredUsers := make([]auth.UserAuth, 0)
		for _, user := range users {
			if user.Username == request.UserName {
				filteredUsers = append(filteredUsers, user)
			}
		}
		users = filteredUsers
	}

	// Encode response
	encodedUsers, err := pmCommon.Encoder(pmCommon.GOB, users)
	if err != nil {
		log.Error("Failed to encode user auth info: ", err)
		return returnError(fmt.Sprintf("failed to encode user auth info: %v", err))
	}

	return encodedUsers, nil
}

func returnError(errMsg string) (interface{}, error) {
	errorResponse := &auth.AuthResponse{
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
