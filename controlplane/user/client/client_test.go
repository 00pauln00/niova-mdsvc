package client

import (
	"os"
	"testing"

	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newClient(t *testing.T) (*Client, func()) {
	t.Helper()

	clusterID := os.Getenv("RAFT_ID")
	if clusterID == "" {
		log.Fatal("RAFT_ID env variable not set")
	}

	configPath := os.Getenv("GOSSIP_NODES_PATH")
	if configPath == "" {
		log.Fatal("GOSSIP_NODES_PATH env variable not set")
	}

	cfg := Config{
		AppUUID:          uuid.New().String(),
		RaftUUID:         clusterID,
		GossipConfigPath: configPath,
	}

	c, tearDown := New(cfg)
	if c == nil {
		t.Fatal("failed to initialize auth client")
	}
	return c, tearDown
}

func TestCreateUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	user := &userlib.UserReq{
		Username: username,
		// UserID is generated server-side
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.SecretKey)
	assert.NotEmpty(t, resp.UserID) // UUID should be generated
	assert.Equal(t, username, resp.Username)
	assert.Equal(t, userlib.StatusActive, resp.Status)
	assert.Equal(t, userlib.DefaultUserRole, resp.UserRole)

	t.Logf("Created user %s with ID %s", username, resp.UserID)
}

func TestCreateUserWithRole(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	user := &userlib.UserReq{
		Username: username,
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.NotEmpty(t, resp.SecretKey)
	assert.NotEmpty(t, resp.UserID)
	assert.Equal(t, username, resp.Username)
	assert.Equal(t, userlib.StatusActive, resp.Status)
	assert.Equal(t, userlib.DefaultUserRole, resp.UserRole)

	t.Logf("Created user %s with ID %s", username, resp.UserID)
}

func TestListUsers(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	// Create a user
	user := &userlib.UserReq{
		Username: username,
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// Retrieve the user by ID
	req := userlib.GetReq{UserID: resp.UserID}
	users, err := c.ListUsers(req)
	assert.NoError(t, err)
	assert.NotNil(t, users)
	require.Greater(t, len(users), 0)
	assert.Equal(t, resp.UserID, users[0].UserID)
	assert.Equal(t, username, users[0].Username)
}

func TestGetUserByID(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]

	createUser := &userlib.UserReq{
		Username: username,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.Equal(t, userlib.DefaultUserRole, createResp.UserRole)

	req := userlib.GetReq{UserID: createResp.UserID}
	users, err := c.ListUsers(req)
	assert.NoError(t, err)
	require.Len(t, users, 1)

	got := users[0]
	assert.Equal(t, createResp.UserID, got.UserID)
	assert.Equal(t, username, got.Username)

	t.Logf("Retrieved user %s with ID %s", username, createResp.UserID)
}

func TestUpdateUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a user first
	originalUsername := "original_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username: originalUsername,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	// Update username
	newUsername := "updated_" + uuid.New().String()[:8]
	updateUser := &userlib.UserReq{
		UserID:   createResp.UserID,
		Username: newUsername,
	}

	updateResp, err := c.UpdateUser(updateUser)
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, createResp.UserID, updateResp.UserID)
	assert.Equal(t, newUsername, updateResp.Username)
	assert.Empty(t, updateResp.SecretKey) // Secret key not returned on update

	t.Logf("Updated user %s: %s -> %s", createResp.UserID, originalUsername, newUsername)
}

func TestUserRoleDerivedFromIsAdmin(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a regular user
	username := "roletest_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username: username,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.Equal(t, userlib.DefaultUserRole, createResp.UserRole) // Role should be "user"

	t.Logf("Created user %s with role %s", createResp.UserID, createResp.UserRole)
}

func TestUpdateUserNotFound(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Try to update non-existent user with a random UUID
	nonExistentID := uuid.New().String()
	updateUser := &userlib.UserReq{
		UserID:   nonExistentID,
		Username: "newname",
	}

	resp, err := c.UpdateUser(updateUser)

	// Should fail - user doesn't exist
	failed := err != nil || (resp != nil && !resp.Success)
	assert.True(t, failed, "expected update of non-existent user to fail")

	t.Logf("UpdateUser correctly failed for non-existent user %s", nonExistentID)
}

func TestGetUserReturnsDecryptedSecretKey(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "secrettest_" + uuid.New().String()[:8]

	// Create a user
	createUser := &userlib.UserReq{
		Username: username,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.SecretKey, "CreateUser should return secret key")

	// Store the original secret key from create response
	originalSecretKey := createResp.SecretKey

	// Get the user and verify secret key is returned decrypted
	getResp, err := c.GetUser(createResp.UserID)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.NotEmpty(t, getResp.SecretKey, "GetUser should return decrypted secret key")
	assert.Equal(t, originalSecretKey, getResp.SecretKey, "GetUser should return same secret key as CreateUser")

	t.Logf("Verified secret key returned correctly for user %s", createResp.UserID)
}

func TestGetUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	users, err := c.ListUsers(userlib.GetReq{})
	assert.NoError(t, err, "error not expect")

	t.Logf("Verified user get call, got: %v users", len(users))
}

func TestCreateAdminUser(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	// Create admin user with custom parameters
	req := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: "my-custom-admin-secret",
	}

	resp, err := c.CreateAdminUser(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Success)
	assert.Equal(t, userlib.AdminUsername, resp.Username)
	assert.Equal(t, uuid.Nil.String(), resp.UserID)
	assert.NotEmpty(t, resp.SecretKey)
	assert.Equal(t, userlib.AdminUserRole, resp.UserRole)

	t.Logf("Admin user created/exists with ID: %s", resp.UserID)

	// Call again - should be idempotent (returns existing admin)
	resp2, err := c.CreateAdminUser(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
	assert.True(t, resp2.Success)
	assert.Equal(t, resp.UserID, resp2.UserID)

	t.Logf("CreateAdminUser is idempotent - second call succeeded")
}

func TestUpdateAdminSecretKey(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	// First ensure admin user exists
	adminReq := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: "initial-secret-key",
	}
	adminResp, err := c.CreateAdminUser(adminReq)
	assert.NoError(t, err)
	assert.NotNil(t, adminResp)
	assert.True(t, adminResp.Success)

	originalSecretKey := adminResp.SecretKey
	t.Logf("Original admin secret key: %s", originalSecretKey)

	// Update the admin secret key
	newSecretKey := "my-new-custom-secret-key-12345"
	updateResp, err := c.UpdateAdminSecretKey(adminResp.UserID, newSecretKey)
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, newSecretKey, updateResp.SecretKey)

	t.Logf("Admin secret key updated successfully")

	// Verify by getting the user
	getResp, err := c.GetUser(adminResp.UserID)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.Equal(t, newSecretKey, getResp.SecretKey)

	t.Logf("Verified admin secret key was updated correctly")
}

func TestUpdateSecretKeyNonAdminFails(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a regular (non-admin) user
	username := "regularuser_" + uuid.New().String()[:8]
	createResp, err := c.CreateUser(&userlib.UserReq{
		Username: username,
	})
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	// Try to update secret key for non-admin user - should fail
	updateResp, err := c.UpdateAdminSecretKey(createResp.UserID, "new-secret")

	// Should fail because user is not admin
	failed := err != nil || (updateResp != nil && !updateResp.Success)
	assert.True(t, failed, "expected secret key update to fail for non-admin user")

	t.Logf("UpdateAdminSecretKey correctly rejected non-admin user")
}

func TestGetUserByUsername(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a user with unique username
	username := "byname_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username: username,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	// Retrieve user by username using secondary index
	getResp, err := c.GetUserByUsername(username)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.Equal(t, createResp.UserID, getResp.UserID)
	assert.Equal(t, username, getResp.Username)
	assert.NotEmpty(t, getResp.SecretKey)

	t.Logf("Retrieved user by username %s -> ID %s", username, getResp.UserID)
}

func TestGetUserByUsernameNotFound(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Try to get a non-existent username
	nonExistentUsername := "nonexistent_" + uuid.New().String()[:8]
	resp, err := c.GetUserByUsername(nonExistentUsername)

	// Should fail - username doesn't exist
	assert.Error(t, err)
	assert.Nil(t, resp)

	t.Logf("GetUserByUsername correctly failed for non-existent username %s", nonExistentUsername)
}

func TestUpdateUserUsernameAlreadyTaken(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create first user
	username1 := "user1_" + uuid.New().String()[:8]
	createUser1 := &userlib.UserReq{
		Username: username1,
	}
	createResp1, err := c.CreateUser(createUser1)
	assert.NoError(t, err)
	assert.True(t, createResp1.Success)
	assert.NotEmpty(t, createResp1.UserID)

	// Create second user
	username2 := "user2_" + uuid.New().String()[:8]
	createUser2 := &userlib.UserReq{
		Username: username2,
	}
	createResp2, err := c.CreateUser(createUser2)
	assert.NoError(t, err)
	assert.True(t, createResp2.Success)
	assert.NotEmpty(t, createResp2.UserID)

	// Try to update first user's username to second user's username (already taken)
	updateUser := &userlib.UserReq{
		UserID:   createResp1.UserID,
		Username: username2, // This username is already taken by user2
	}

	updateResp, err := c.UpdateUser(updateUser)

	// Should fail - username already exists
	failed := err != nil || (updateResp != nil && !updateResp.Success)
	assert.True(t, failed, "expected update to fail when username is already taken")

	if updateResp != nil && updateResp.Error != "" {
		assert.Contains(t, updateResp.Error, "already exists", "error should indicate username already exists")
	}

	// Verify original user still has original username
	getResp, err := c.GetUser(createResp1.UserID)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.Equal(t, username1, getResp.Username, "original username should remain unchanged")

	t.Logf("UpdateUser correctly rejected username change to already taken username '%s'", username2)
}

func TestUpdateUserUsernameIndexDeleted(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create a user with an initial username
	oldUsername := "oldname_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username: oldUsername,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	// Verify we can look up by old username
	getByOldName, err := c.GetUserByUsername(oldUsername)
	assert.NoError(t, err)
	assert.NotNil(t, getByOldName)
	assert.Equal(t, createResp.UserID, getByOldName.UserID)

	// Update to a new username
	newUsername := "newname_" + uuid.New().String()[:8]
	updateUser := &userlib.UserReq{
		UserID:   createResp.UserID,
		Username: newUsername,
	}

	updateResp, err := c.UpdateUser(updateUser)
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, newUsername, updateResp.Username)

	// Verify the NEW username index works
	getByNewName, err := c.GetUserByUsername(newUsername)
	assert.NoError(t, err)
	assert.NotNil(t, getByNewName)
	assert.Equal(t, createResp.UserID, getByNewName.UserID)
	assert.Equal(t, newUsername, getByNewName.Username)

	// Verify the OLD username index was deleted (should return not found)
	getByOldNameAfter, err := c.GetUserByUsername(oldUsername)
	assert.Error(t, err, "expected error when looking up deleted old username")
	assert.Nil(t, getByOldNameAfter, "expected nil response for deleted old username index")

	t.Logf("Username index correctly deleted: old=%s (deleted), new=%s (works)", oldUsername, newUsername)
}

func TestUpdateUserUsernameIndexDeletedCanBeReused(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create first user with usernameA
	usernameA := "reusable_" + uuid.New().String()[:8]
	createUser1 := &userlib.UserReq{
		Username: usernameA,
	}

	createResp1, err := c.CreateUser(createUser1)
	assert.NoError(t, err)
	assert.True(t, createResp1.Success)

	// Change first user's username from A to B
	usernameB := "changed_" + uuid.New().String()[:8]
	updateUser1 := &userlib.UserReq{
		UserID:   createResp1.UserID,
		Username: usernameB,
	}

	updateResp1, err := c.UpdateUser(updateUser1)
	assert.NoError(t, err)
	assert.True(t, updateResp1.Success)

	// Now create a NEW user with the old username (usernameA)
	// This should succeed because the old username index was properly deleted
	createUser2 := &userlib.UserReq{
		Username: usernameA, // Reuse the old username
	}

	createResp2, err := c.CreateUser(createUser2)
	assert.NoError(t, err, "should be able to create user with previously used username")
	assert.NotNil(t, createResp2)
	assert.True(t, createResp2.Success, "creating user with reused username should succeed")
	assert.NotEqual(t, createResp1.UserID, createResp2.UserID, "new user should have different ID")

	// Verify the reused username points to the new user
	getByReusedName, err := c.GetUserByUsername(usernameA)
	assert.NoError(t, err)
	assert.NotNil(t, getByReusedName)
	assert.Equal(t, createResp2.UserID, getByReusedName.UserID, "reused username should point to new user")

	t.Logf("Username '%s' successfully reused after being freed by user %s", usernameA, createResp1.UserID)
}

func TestLoginUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	// Create user
	loginUser := &userlib.UserReq{
		Username: "loginUser" + uuid.New().String()[:8],
	}
	createResp, err := c.CreateUser(loginUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	// Login user with user created
	loginResp, err := c.Login(loginUser.Username, createResp.SecretKey)
	assert.NoError(t, err, "newly created user should be able to login")
	assert.True(t, loginResp.Success)
	assert.Equal(t, loginResp.Error, "", "no error expected")
	assert.NotEmpty(t, loginResp.AccessToken, "accessToken can not be empty")

	// Login with invalid user
	loginResp, err = c.Login("nouser", "password")
	assert.Error(t, err, "login failed: invalid credentials")
}
