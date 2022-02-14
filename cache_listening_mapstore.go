package smt

var _ CommitMapStore = &CacheListeningMapStore{}

// CacheListeningMapStore wraps another (persistent) MapStore with an in-memory cache
// and can be initialized with listener channel that receives the key/value pairs on Commit
type CacheListeningMapStore struct {
	cache      map[string]operation // map of key to operation
	listenChan chan KVPair
	db         MapStore
}

// KVPair struct for listening to KVPairs
// Delete flag is included so that we can signify to an external service when to delete a given KVPair
type KVPair struct {
	Key    string
	Value  []byte
	Delete bool
}

// NewCacheListeningMapStore creates a new empty CacheListeningMapStore.
func NewCacheListeningMapStore(db MapStore, listener chan KVPair, limit uint64) *CacheListeningMapStore {
	return &CacheListeningMapStore{
		cache:      make(map[string]operation, limit),
		db:         db,
		listenChan: listener,
	}
}

// Get gets the value for a key.
func (cm *CacheListeningMapStore) Get(key []byte) ([]byte, error) {
	if op, ok := cm.cache[string(key)]; ok {
		if op.delete {
			return nil, &InvalidKeyError{Key: key}
		}
		return op.val, nil
	}
	return cm.db.Get(key)
}

// Set updates the value for a key.
func (cm *CacheListeningMapStore) Set(key []byte, value []byte) error {
	cm.cache[string(key)] = operation{val: value, delete: false}
	return nil
}

// Delete deletes a key.
func (cm *CacheListeningMapStore) Delete(key []byte) error {
	cm.cache[string(key)] = operation{val: nil, delete: true}
	return nil
}

func (cm *CacheListeningMapStore) Commit() error {
	for k, op := range cm.cache {
		key := []byte(k)
		if op.delete {
			if err := cm.db.Delete(key); err != nil {
				return err
			}
		} else {
			if err := cm.db.Set(key, op.val); err != nil {
				return err
			}
		}
		cm.listenChan <- KVPair{k, op.val, op.delete}
	}
	return nil
}
