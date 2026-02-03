package userserver

import (
	"fmt"

	userlib "github.com/00pauln00/niova-mdsvc/controlplane/user/lib"
	pumiceFunc "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
)

type EntitySerializer interface {
	Serialize(entity Entity, commitChgs *[]pumiceFunc.CommitChg)
}

func SerializeEntities[T Entity](entity T, serializer EntitySerializer) []pumiceFunc.CommitChg {
	commitChgs := make([]pumiceFunc.CommitChg, 0)
	serializer.Serialize(entity, &commitChgs)
	return commitChgs
}

// UserSerializer implements EntitySerializer for User
type UserSerializer struct{}

// Serialize serializes a User entity into commit changes for RocksDB
// - /u/<UserId>/<username>
// - /u/<UserId>/<secretkey>
// - /u/<UserId>/<usertype>/
// - /u/<UserId>/<userrole>
// - /u/<UserId>/st/<Active/inactive>
func (UserSerializer) Serialize(entity Entity, commitChgs *[]pumiceFunc.CommitChg) {
	user := entity.(*userlib.User)

	// Base Key: /u/<UserId> (UserID is UUID string)
	baseKey := fmt.Sprintf("%s/%s", userKeyPrefix, user.UserID)

	// /u/<UserId>/<username>
	if user.Username != "" {
		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", baseKey, usernameField)),
			Value: []byte(user.Username),
		})
	}

	// /u/<UserId>/<secretkey>
	if user.SecretKey != "" {
		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", baseKey, secretKeyField)),
			Value: []byte(user.SecretKey),
		})
	}

	// /u/<UserId>/<usertype>/ - admin or non-admin
	userType := "non-admin"
	if user.IsAdmin {
		userType = "admin"
	}
	*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
		Key:   []byte(fmt.Sprintf("%s/%s", baseKey, userTypeField)),
		Value: []byte(userType),
	})

	// /u/<UserId>/<userrole>
	if user.UserRole != "" {
		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s", baseKey, userRoleField)),
			Value: []byte(user.UserRole),
		})
	}

	// /u/<UserId>/st/<Active/inactive>
	*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
		Key:   []byte(fmt.Sprintf("%s/%s", baseKey, statusField)),
		Value: []byte{byte(user.Status)},
	})

	// Secondary index: /u-idx/un/<username> -> <UserId>
	if user.Username != "" {
		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", userIndexPrefix, usernameIdxPrefix, user.Username)),
			Value: []byte(user.UserID),
		})
	}
}

// SerializeUser is helper function to serialize a User entity
func SerializeUser(user *userlib.User) []pumiceFunc.CommitChg {
	return SerializeEntities[*userlib.User](user, UserSerializer{})
}

// SerializeUsernameIndexDelete generates a delete operation for the old username index.
func SerializeUsernameIndexDelete(oldUsername string) pumiceFunc.CommitChg {
	return pumiceFunc.CommitChg{
		Key:   []byte(fmt.Sprintf("%s/%s/%s", userIndexPrefix, usernameIdxPrefix, oldUsername)),
		Value: []byte{}, // Empty value signals delete
	}
}
