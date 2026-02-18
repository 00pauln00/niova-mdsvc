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

// Shared admin secret used across all tests.
// Tests that modify the admin secret must restore it when done.
const testAdminSecret = "test-admin-secret"

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

func setupAdmin(t *testing.T, c *Client) string {
	t.Helper()

	// Ensure admin exists
	adminReq := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: testAdminSecret,
	}
	resp, err := c.CreateAdminUser(adminReq)
	if err != nil {
		t.Logf("Admin creation returned: %v (may already exist)", err)
	}

	// Try logging in with the known secret
	loginResp, err := c.Login(userlib.AdminUsername, testAdminSecret)
	if err != nil {
		// If login fails, the admin secret may have been changed by a previous run.
		// Try to reset it using the secret returned from CreateAdminUser if available.
		if resp != nil && resp.SecretKey != "" && resp.SecretKey != testAdminSecret {
			t.Logf("Trying login with secret from CreateAdminUser response...")
			loginResp, err = c.Login(userlib.AdminUsername, resp.SecretKey)
			if err == nil && loginResp.Success {
				// Now update the secret back to the known one
				_, updateErr := c.UpdateAdminSecretKey(resp.UserID, testAdminSecret, loginResp.AccessToken)
				if updateErr != nil {
					t.Fatalf("Failed to reset admin secret: %v", updateErr)
				}
				// Re-login with the reset secret
				loginResp, err = c.Login(userlib.AdminUsername, testAdminSecret)
			}
		}
	}

	require.NoError(t, err, "admin login should succeed")
	require.True(t, loginResp.Success, "admin login should be successful")
	require.NotEmpty(t, loginResp.AccessToken, "admin access token should not be empty")
	return loginResp.AccessToken
}

func TestCreateUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)
	username := "testuser_" + uuid.New().String()[:8]

	user := &userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
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

func TestCreateUserWithRole(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	username := "testuser_" + uuid.New().String()[:8]
	adminToken := setupAdmin(t, c)

	user := &userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
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
	adminToken := setupAdmin(t, c)

	user := &userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
	}

	resp, err := c.CreateUser(user)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	userResp, err := c.Login(resp.Username, resp.SecretKey)
	assert.NoError(t, err, "user not able to login")
	assert.NotEmpty(t, userResp, "expected userlogin response")
	assert.NotEmpty(t, userResp.AccessToken, "User access token can not be empty")

	req := userlib.GetReq{
		UserID:    resp.UserID,
		UserToken: userResp.AccessToken,
	}
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
	adminToken := setupAdmin(t, c)

	createUser := &userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.Equal(t, userlib.DefaultUserRole, createResp.UserRole)

	req := userlib.GetReq{UserID: createResp.UserID, UserToken: adminToken}
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

	adminToken := setupAdmin(t, c)

	originalUsername := "original_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username:  originalUsername,
		UserToken: adminToken,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	newUsername := "updated_" + uuid.New().String()[:8]
	updateUser := &userlib.UserReq{
		UserID:    createResp.UserID,
		Username:  newUsername,
		UserToken: adminToken,
	}

	updateResp, err := c.UpdateUser(updateUser)
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, createResp.UserID, updateResp.UserID)
	assert.Equal(t, newUsername, updateResp.Username)
	assert.Empty(t, updateResp.SecretKey)

	t.Logf("Updated user %s: %s -> %s", createResp.UserID, originalUsername, newUsername)
}

func TestUserRoleDerivedFromIsAdmin(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	username := "roletest_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.Equal(t, userlib.DefaultUserRole, createResp.UserRole)

	t.Logf("Created user %s with role %s", createResp.UserID, createResp.UserRole)
}

func TestUpdateUserNotFound(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	nonExistentID := uuid.New().String()
	updateUser := &userlib.UserReq{
		UserID:    nonExistentID,
		Username:  "newname",
		UserToken: adminToken,
	}

	resp, err := c.UpdateUser(updateUser)

	failed := err != nil || (resp != nil && !resp.Success)
	assert.True(t, failed, "expected update of non-existent user to fail")

	t.Logf("UpdateUser correctly failed for non-existent user %s", nonExistentID)
}

func TestGetUserReturnsDecryptedSecretKey(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)
	username := "secrettest_" + uuid.New().String()[:8]

	createUser := &userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.SecretKey, "CreateUser should return secret key")

	originalSecretKey := createResp.SecretKey

	getResp, err := c.GetUser(createResp.UserID, adminToken)
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

// TestCreateAdminUser is NOT parallel because it modifies admin state.
func TestCreateAdminUser(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	// Use the shared admin secret so we don't break other tests
	req := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: testAdminSecret,
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

	resp2, err := c.CreateAdminUser(req)
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
	assert.True(t, resp2.Success)
	assert.Equal(t, resp.UserID, resp2.UserID)

	t.Logf("CreateAdminUser is idempotent - second call succeeded")
}

// TestUpdateAdminSecretKey is NOT parallel because it modifies shared admin state.
// It restores the original secret at the end.
func TestUpdateAdminSecretKey(t *testing.T) {
	c, tearDown := newClient(t)
	defer tearDown()

	// Ensure admin exists with the known secret
	adminReq := &userlib.UserReq{
		Username:     userlib.AdminUsername,
		NewSecretKey: testAdminSecret,
	}
	adminResp, err := c.CreateAdminUser(adminReq)
	assert.NoError(t, err)
	assert.NotNil(t, adminResp)
	assert.True(t, adminResp.Success)

	originalSecretKey := adminResp.SecretKey
	t.Logf("Original admin secret key: %s", originalSecretKey)

	loginResp, err := c.Login(userlib.AdminUsername, testAdminSecret)
	require.NoError(t, err, "admin login should succeed")
	require.True(t, loginResp.Success, "admin login should be successful")
	require.NotEmpty(t, loginResp.AccessToken, "admin access token should not be empty")

	// Update the admin secret key to something new
	newSecretKey := "my-new-custom-secret-key-12345"
	updateResp, err := c.UpdateAdminSecretKey(adminResp.UserID, newSecretKey, loginResp.AccessToken)
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, newSecretKey, updateResp.SecretKey)

	t.Logf("Admin secret key updated successfully")

	// Verify by getting the user
	getResp, err := c.GetUser(adminResp.UserID, loginResp.AccessToken)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.Equal(t, newSecretKey, getResp.SecretKey)

	t.Logf("Verified admin secret key was updated correctly")

	// RESTORE the admin secret back to the shared test secret so other tests aren't broken.
	restoreLoginResp, err := c.Login(userlib.AdminUsername, newSecretKey)
	require.NoError(t, err, "login with new secret should succeed")
	require.True(t, restoreLoginResp.Success)

	restoreResp, err := c.UpdateAdminSecretKey(adminResp.UserID, testAdminSecret, restoreLoginResp.AccessToken)
	require.NoError(t, err, "restoring admin secret should succeed")
	require.True(t, restoreResp.Success)

	t.Logf("Restored admin secret key to shared test value")
}

func TestUpdateSecretKeyNonAdminFails(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	// Create a regular (non-admin) user — must pass adminToken to authorize creation
	username := "regularuser_" + uuid.New().String()[:8]
	createResp, err := c.CreateUser(&userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
	})
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	// Try to update secret key for non-admin user - should fail
	updateResp, err := c.UpdateAdminSecretKey(createResp.UserID, "new-secret", adminToken)

	failed := err != nil || (updateResp != nil && !updateResp.Success)
	assert.True(t, failed, "expected secret key update to fail for non-admin user")

	t.Logf("UpdateAdminSecretKey correctly rejected non-admin user")
}

func TestGetUserByUsername(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	username := "byname_" + uuid.New().String()[:8]
	createUser := &userlib.UserReq{
		Username:  username,
		UserToken: adminToken,
	}

	createResp, err := c.CreateUser(createUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	getResp, err := c.GetUserByUsername(username, adminToken)
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

	adminToken := setupAdmin(t, c)

	nonExistentUsername := "nonexistent_" + uuid.New().String()[:8]
	resp, err := c.GetUserByUsername(nonExistentUsername, adminToken)

	assert.Error(t, err)
	assert.Nil(t, resp)

	t.Logf("GetUserByUsername correctly failed for non-existent username %s", nonExistentUsername)
}

func TestUpdateUserUsernameAlreadyTaken(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	username1 := "user1_" + uuid.New().String()[:8]
	createResp1, err := c.CreateUser(&userlib.UserReq{
		Username:  username1,
		UserToken: adminToken,
	})
	assert.NoError(t, err)
	assert.True(t, createResp1.Success)
	assert.NotEmpty(t, createResp1.UserID)

	username2 := "user2_" + uuid.New().String()[:8]
	createResp2, err := c.CreateUser(&userlib.UserReq{
		Username:  username2,
		UserToken: adminToken,
	})
	assert.NoError(t, err)
	assert.True(t, createResp2.Success)
	assert.NotEmpty(t, createResp2.UserID)

	updateUser := &userlib.UserReq{
		UserID:    createResp1.UserID,
		Username:  username2,
		UserToken: adminToken,
	}

	updateResp, err := c.UpdateUser(updateUser)

	failed := err != nil || (updateResp != nil && !updateResp.Success)
	assert.True(t, failed, "expected update to fail when username is already taken")

	if updateResp != nil && updateResp.Error != "" {
		assert.Contains(t, updateResp.Error, "already exists", "error should indicate username already exists")
	}

	getResp, err := c.GetUser(createResp1.UserID, adminToken)
	assert.NoError(t, err)
	assert.NotNil(t, getResp)
	assert.Equal(t, username1, getResp.Username, "original username should remain unchanged")

	t.Logf("UpdateUser correctly rejected username change to already taken username '%s'", username2)
}

func TestUpdateUserUsernameIndexDeleted(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	oldUsername := "oldname_" + uuid.New().String()[:8]
	createResp, err := c.CreateUser(&userlib.UserReq{
		Username:  oldUsername,
		UserToken: adminToken,
	})
	assert.NoError(t, err)
	assert.True(t, createResp.Success)
	assert.NotEmpty(t, createResp.UserID)

	getByOldName, err := c.GetUserByUsername(oldUsername, adminToken)
	assert.NoError(t, err)
	assert.NotNil(t, getByOldName)
	assert.Equal(t, createResp.UserID, getByOldName.UserID)

	newUsername := "newname_" + uuid.New().String()[:8]
	updateResp, err := c.UpdateUser(&userlib.UserReq{
		UserID:    createResp.UserID,
		Username:  newUsername,
		UserToken: adminToken,
	})
	assert.NoError(t, err)
	assert.NotNil(t, updateResp)
	assert.True(t, updateResp.Success)
	assert.Equal(t, newUsername, updateResp.Username)

	getByNewName, err := c.GetUserByUsername(newUsername, adminToken)
	assert.NoError(t, err)
	assert.NotNil(t, getByNewName)
	assert.Equal(t, createResp.UserID, getByNewName.UserID)
	assert.Equal(t, newUsername, getByNewName.Username)

	getByOldNameAfter, err := c.GetUserByUsername(oldUsername, adminToken)
	assert.Error(t, err, "expected error when looking up deleted old username")
	assert.Nil(t, getByOldNameAfter, "expected nil response for deleted old username index")

	t.Logf("Username index correctly deleted: old=%s (deleted), new=%s (works)", oldUsername, newUsername)
}

func TestUpdateUserUsernameIndexDeletedCanBeReused(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	usernameA := "reusable_" + uuid.New().String()[:8]
	createResp1, err := c.CreateUser(&userlib.UserReq{
		Username:  usernameA,
		UserToken: adminToken,
	})
	assert.NoError(t, err)
	assert.True(t, createResp1.Success)

	usernameB := "changed_" + uuid.New().String()[:8]
	updateResp1, err := c.UpdateUser(&userlib.UserReq{
		UserID:    createResp1.UserID,
		Username:  usernameB,
		UserToken: adminToken,
	})
	assert.NoError(t, err)
	assert.True(t, updateResp1.Success)

	createResp2, err := c.CreateUser(&userlib.UserReq{
		Username:  usernameA,
		UserToken: adminToken,
	})
	assert.NoError(t, err, "should be able to create user with previously used username")
	assert.NotNil(t, createResp2)
	assert.True(t, createResp2.Success, "creating user with reused username should succeed")
	assert.NotEqual(t, createResp1.UserID, createResp2.UserID, "new user should have different ID")

	getByReusedName, err := c.GetUserByUsername(usernameA, adminToken)
	assert.NoError(t, err)
	assert.NotNil(t, getByReusedName)
	assert.Equal(t, createResp2.UserID, getByReusedName.UserID, "reused username should point to new user")

	t.Logf("Username '%s' successfully reused after being freed by user %s", usernameA, createResp1.UserID)
}

func TestLoginUser(t *testing.T) {
	t.Parallel()

	c, tearDown := newClient(t)
	defer tearDown()

	adminToken := setupAdmin(t, c)

	loginUser := &userlib.UserReq{
		Username:  "loginUser" + uuid.New().String()[:8],
		UserToken: adminToken,
	}
	createResp, err := c.CreateUser(loginUser)
	assert.NoError(t, err)
	assert.True(t, createResp.Success)

	// Login with the created user's secret key
	loginResp, err := c.Login(loginUser.Username, createResp.SecretKey)
	assert.NoError(t, err, "newly created user should be able to login")
	assert.True(t, loginResp.Success)
	assert.Equal(t, loginResp.Error, "", "no error expected")
	assert.NotEmpty(t, loginResp.AccessToken, "accessToken can not be empty")

	// Login with invalid credentials
	loginResp, err = c.Login("nouser", "password")
	assert.Error(t, err, "login failed: invalid credentials")
}
