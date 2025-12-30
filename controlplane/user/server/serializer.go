package userserver

import (
	"encoding/binary"
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
// - /u/<UserId>/cap/CP/<Permissions>
// - /u/<UserId>/cap/Nisd/<Permissions>
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

	// /u/<UserId>/cap/CP/<Permissions> and /u/<UserId>/cap/Nisd/<Permissions>
	for _, cap := range user.Capabilities {
		permBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(permBytes, cap.Perm)
		*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
			Key:   []byte(fmt.Sprintf("%s/%s/%s", baseKey, capabilityKeyPrefix, cap.Service)),
			Value: permBytes,
		})
	}

	// /u/<UserId>/st/<Active/inactive>
	*commitChgs = append(*commitChgs, pumiceFunc.CommitChg{
		Key:   []byte(fmt.Sprintf("%s/%s", baseKey, statusField)),
		Value: []byte{byte(user.Status)},
	})
}

// SerializeUser is helper function to serialize a User entity
func SerializeUser(user *userlib.User) []pumiceFunc.CommitChg {
	return SerializeEntities[*userlib.User](user, UserSerializer{})
}
