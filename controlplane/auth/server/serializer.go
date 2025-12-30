package authserver

import (
	"fmt"
	"time"

	auth "github.com/00pauln00/niova-mdsvc/controlplane/auth/lib"
	pumiceFunc "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
)

type EntitySerializer interface {
	Serialize(entity Entity, commitChgs *[]pumiceFunc.CommitChg, entityKey string)
}

func SerializeEntities[T Entity](entity T, serializer EntitySerializer, entityKey string) []pumiceFunc.CommitChg {
	commitChgs := make([]pumiceFunc.CommitChg, 0)
	serializer.Serialize(entity, &commitChgs, entityKey)
	return commitChgs
}

// UserAuthSerializer implements EntitySerializer for UserAuth
type UserAuthSerializer struct{}

// Serialize serializes a UserAuth entity into commit changes for RocksDB
// Schema: auth/users/{userID}/{field} : {value}
// -- auth/usernames/{username}
// -- auth/{authSecretKeysKey}/{secretKey}
func (UserAuthSerializer) Serialize(entity Entity, commitChgs *[]pumiceFunc.CommitChg, entityKey string) {
	user := entity.(*auth.UserAuth)

	// Base Key: {authKey}/{entityKey}/{userID}
	baseKey := fmt.Sprintf("%s/%s/%s", authKey, entityKey, user.UserID)

	// Populate all user fields
	for _, field := range []string{secretKeyField, usernameField, statusField, createdAtField, updatedAtField} {
		var value []byte
		switch field {
		case secretKeyField:
			value = []byte(user.SecretKey)
		case usernameField:
			value = []byte(user.Username)
		case statusField:
			value = []byte{byte(user.Status)}
		case createdAtField:
			timeStr := user.CreatedAt.Format(time.RFC3339)
			if timeStr != "" {
				value = []byte(timeStr)
			}
		case updatedAtField:
			timeStr := user.UpdatedAt.Format(time.RFC3339)
			if timeStr != "" {
				value = []byte(timeStr)
			}
		default:
			continue
		}

		// Skip empty values
		if len(value) == 0 {
			continue
		}

		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", baseKey, field)),
			Value: value,
		})
	}

	// Add username index
	if user.Username != "" {
		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/usernames/%s", authKey, user.Username)),
			Value: []byte(user.UserID),
		})
	}

	// Add secret key index
	if user.SecretKey != "" {
		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", authKey, authSecretKeysKey, user.SecretKey)),
			Value: []byte(user.UserID),
		})
	}
}

// SerializeAuthUser is helper function to serialize a UserAuth entity
func SerializeAuthUser(user *auth.UserAuth) []pumiceFunc.CommitChg {
	return SerializeEntities[*auth.UserAuth](user, UserAuthSerializer{}, authUsersKey)
}
