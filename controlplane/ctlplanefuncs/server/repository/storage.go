package repository

import (
	"fmt"

	log "github.com/00pauln00/niova-lookout/pkg/xlog"
	funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

type StorageRepository struct {
	columnFamily string
}

func NewStorageRepository(cf string) *StorageRepository {
	return &StorageRepository{columnFamily: cf}
}

func (r *StorageRepository) RangeReadPrefix(store storageiface.DataStore, prefix string, bufSize int64) (*storageiface.RangeReadResult, error) {
	return store.RangeRead(storageiface.RangeReadArgs{
		Selector: r.columnFamily,
		Key:      prefix,
		BufSize:  bufSize,
		Prefix:   prefix,
	})
}

func (r *StorageRepository) Read(store storageiface.DataStore, key string) ([]byte, error) {
	return store.Read(key, r.columnFamily)
}

func (r *StorageRepository) ApplyChanges(store storageiface.DataStore, chgs []funclib.CommitChg) error {
	return applyKV(chgs, store, r.columnFamily)
}

func (r *StorageRepository) DeleteChanges(store storageiface.DataStore, chgs []funclib.CommitChg) error {
	for _, chg := range chgs {
		log.Debug("Deleting key: ", string(chg.Key))
		if err := store.Delete(string(chg.Key), r.columnFamily); err != nil {
			log.Error("Failed to delete key: ", string(chg.Key), " err: ", err)
			return fmt.Errorf("failed to delete key: %s, err: %v", string(chg.Key), err)
		}
	}
	return nil
}

func applyKV(chgs []funclib.CommitChg, ds storageiface.DataStore, cf string) error {
	for _, chg := range chgs {
		var err error
		switch chg.Op {
		case funclib.OpApply:
			log.Debug("Applying change: ", string(chg.Key), " -> ", string(chg.Value))
			err = ds.Write(string(chg.Key), string(chg.Value), cf)
		case funclib.OpDelete:
			log.Debug("Deleting key: ", string(chg.Key))
			err = ds.Delete(string(chg.Key), cf)
		default:
			log.Error("Unknown operation type: ", chg.Op, " for key: ", string(chg.Key))
			return fmt.Errorf("unknown operation type %d for key %s", chg.Op, string(chg.Key))
		}
		if err != nil {
			log.Error("Failed to apply changes for key: ", string(chg.Key), " err: ", err)
			return fmt.Errorf("failed to apply changes for key: %s", string(chg.Key))
		}
	}
	return nil
}
