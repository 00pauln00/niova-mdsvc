package authlib

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserAuth_Validate(t *testing.T) {
	tests := []struct {
		name      string
		userAuth  UserAuth
		wantError bool
		errorMsg  string
	}{
		{
			name: "ValidUser",
			userAuth: UserAuth{
				Username:  "testuser",
				UserID:    uuid.New().String(),
				SecretKey: "test-secret-key",
				Status:    StatusActive,
			},
		},
		{
			name: "ValidUserWithoutUserID",
			userAuth: UserAuth{
				Username:  "testuser",
				SecretKey: "test-secret-key",
				Status:    StatusActive,
			},
		},
		{
			name: "EmptyUsername",
			userAuth: UserAuth{
				Username:  "",
				UserID:    uuid.New().String(),
				SecretKey: "test-secret-key",
				Status:    StatusActive,
			},
			wantError: true,
			errorMsg:  "username cannot be empty",
		},
		{
			name: "WhitespaceUsername",
			userAuth: UserAuth{
				Username:  "   ",
				UserID:    uuid.New().String(),
				SecretKey: "test-secret-key",
				Status:    StatusActive,
			},
			wantError: true,
			errorMsg:  "username cannot be empty",
		},
		{
			name: "InvalidUserIDFormat",
			userAuth: UserAuth{
				Username:  "testuser",
				UserID:    "not-a-uuid",
				SecretKey: "test-secret-key",
				Status:    StatusActive,
			},
			wantError: true,
			errorMsg:  "invalid userID format",
		},
		{
			name: "InvalidStatus",
			userAuth: UserAuth{
				Username:  "testuser",
				UserID:    uuid.New().String(),
				SecretKey: "test-secret-key",
				Status:    99,
			},
			wantError: true,
			errorMsg:  "invalid status",
		},
		{
			name: "EmptyStatusIsValid",
			userAuth: UserAuth{
				Username:  "testuser",
				UserID:    uuid.New().String(),
				SecretKey: "test-secret-key",
			},
		},
		{
			name: "StatusActive",
			userAuth: UserAuth{
				Username: "testuser",
				Status:   StatusActive,
			},
		},
		{
			name: "StatusInactive",
			userAuth: UserAuth{
				Username: "testuser",
				Status:   StatusInactive,
			},
		},
		{
			name: "StatusSuspended",
			userAuth: UserAuth{
				Username: "testuser",
				Status:   StatusSuspended,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.userAuth.Validate()
			if tt.wantError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUserAuth_Init(t *testing.T) {
	t.Run("GeneratesUserIDIfEmpty", func(t *testing.T) {
		user := &UserAuth{
			Username: "testuser",
		}

		err := user.Init()
		require.NoError(t, err)

		assert.NotEmpty(t, user.UserID)
		// Verify it's a valid UUID
		_, err = uuid.Parse(user.UserID)
		assert.NoError(t, err)
	})

	t.Run("PreservesExistingUserID", func(t *testing.T) {
		existingID := uuid.New().String()
		user := &UserAuth{
			Username: "testuser",
			UserID:   existingID,
		}

		err := user.Init()
		require.NoError(t, err)

		assert.Equal(t, existingID, user.UserID, "UserID should not change if already set")
	})

	t.Run("SetsDefaultStatusToActive", func(t *testing.T) {
		user := &UserAuth{
			Username: "testuser",
		}

		err := user.Init()
		require.NoError(t, err)

		assert.Equal(t, StatusActive, user.Status)
	})

	t.Run("PreservesExistingStatus", func(t *testing.T) {
		user := &UserAuth{
			Username: "testuser",
			Status:   StatusSuspended,
		}

		err := user.Init()
		require.NoError(t, err)

		assert.Equal(t, StatusSuspended, user.Status, "Status should not change if already set")
	})

	t.Run("SetsCreatedAtTimestamp", func(t *testing.T) {
		beforeInit := time.Now()
		user := &UserAuth{
			Username: "testuser",
		}

		err := user.Init()
		require.NoError(t, err)
		afterInit := time.Now()

		assert.False(t, user.CreatedAt.IsZero(), "CreatedAt should be set")
		assert.True(t, user.CreatedAt.After(beforeInit) || user.CreatedAt.Equal(beforeInit))
		assert.True(t, user.CreatedAt.Before(afterInit) || user.CreatedAt.Equal(afterInit))
	})

	t.Run("GeneratesUniqueUserIDs", func(t *testing.T) {
		userIDs := make(map[string]bool)

		for i := 0; i < 100; i++ {
			user := &UserAuth{Username: "testuser"}
			err := user.Init()
			require.NoError(t, err)
			assert.False(t, userIDs[user.UserID], "Generated duplicate UserID")
			userIDs[user.UserID] = true
		}

		assert.Equal(t, 100, len(userIDs), "Should generate 100 unique UserIDs")
	})
}

func TestGetAuthReq_Validate(t *testing.T) {
	tests := []struct {
		name      string
		req       GetAuthReq
		wantError bool
		errorMsg  string
	}{
		{
			name: "ValidWithUserID",
			req: GetAuthReq{
				UserID: uuid.New().String(),
			},
		},
		{
			name: "ValidWithUserName",
			req: GetAuthReq{
				UserName: "testuser",
			},
		},
		{
			name: "ValidWithBoth",
			req: GetAuthReq{
				UserID:   uuid.New().String(),
				UserName: "testuser",
			},
		},
		{
			name: "ValidWithNeither",
			req:  GetAuthReq{},
		},
		{
			name: "InvalidUserIDFormat",
			req: GetAuthReq{
				UserID: "not-a-uuid",
			},
			wantError: true,
			errorMsg:  "invalid userID format",
		},
		{
			name: "InvalidUserIDWithValidUserName",
			req: GetAuthReq{
				UserID:   "invalid-uuid",
				UserName: "testuser",
			},
			wantError: true,
			errorMsg:  "invalid userID format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()

			if tt.wantError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUserAuth_FullWorkflow(t *testing.T) {
	t.Run("CreateValidateAndInit", func(t *testing.T) {
		// Create a new user
		user := &UserAuth{
			Username:  "Narendra.M",
			SecretKey: "test-secret-key",
		}

		// Initialize it
		err := user.Init()
		require.NoError(t, err)

		// Validate it
		err = user.Validate()
		require.NoError(t, err)

		// Verify all fields are properly set
		assert.NotEmpty(t, user.UserID)
		assert.Equal(t, "Narendra.M", user.Username)
		assert.Equal(t, "test-secret-key", user.SecretKey)
		assert.Equal(t, StatusActive, user.Status)
		assert.False(t, user.CreatedAt.IsZero())
	})

	t.Run("UpdateUserStatus", func(t *testing.T) {
		user := &UserAuth{
			Username: "Narendra.M",
		}

		err := user.Init()
		require.NoError(t, err)
		assert.Equal(t, StatusActive, user.Status)

		// Update to suspended
		user.Status = StatusSuspended
		err = user.Validate()
		assert.NoError(t, err)

		// Update to inactive
		user.Status = StatusInactive
		err = user.Validate()
		assert.NoError(t, err)
	})
}
