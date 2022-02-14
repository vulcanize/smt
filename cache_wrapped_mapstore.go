package smt

// CommitMapStore composes a Commit interface with a MapStore interface
type CommitMapStore interface {
	MapStore
	Commit() error
}

var _ CommitMapStore = &CachedMapStore{}

// CachedMapStore wraps another (persistent) MapStore with an in-memory cache
type CachedMapStore struct {
	cache map[string]operation // map of key to operation
	db    MapStore
}

type operation struct {
	val    []byte
	delete bool
}

// NewCachedMap creates a new empty CachedMapStore.
func NewCachedMap(db MapStore, limit uint64) *CachedMapStore {
	return &CachedMapStore{
		cache: make(map[string]operation, limit),
		db:    db,
	}
}

// Get gets the value for a key.
func (cm *CachedMapStore) Get(key []byte) ([]byte, error) {
	if op, ok := cm.cache[string(key)]; ok {
		if op.delete {
			return nil, &InvalidKeyError{Key: key}
		}
		return op.val, nil
	}
	return cm.db.Get(key)
}

// Set updates the value for a key.
func (cm *CachedMapStore) Set(key []byte, value []byte) error {
	cm.cache[string(key)] = operation{val: value, delete: false}
	return nil
}

// Delete deletes a key.
func (cm *CachedMapStore) Delete(key []byte) error {
	cm.cache[string(key)] = operation{val: nil, delete: true}
	return nil
}

func (cm *CachedMapStore) Commit() error {
	for k, op := range cm.cache {
		key := []byte(k)
		if op.delete {
			// NOTE: when there is a set then delete on a key that doesn't already exist on disc, when we go to flush those operations
			// we will hit a missing key error which is not the expected behavior since it expects the key to be written to disk
			// from the first operation but instead if it overwritten in memory
			if err := cm.db.Delete(key); err != nil {
				return err
			}
		} else {
			if err := cm.db.Set(key, op.val); err != nil {
				return err
			}
		}
	}
	return nil
}
