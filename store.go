package main

import (
	"fmt"
	"github.com/teejays/clog"
)

// Error variables for various unexpected situations that occur while dealing with Store.
var (
	ERR_KEY_NOT_EXIST   error = fmt.Errorf("KEY NOT FOUND")
	ERR_NOT_TRANSACTION error = fmt.Errorf("TRANSACTION NOT FOUND")
)

// Store is the main data structure that supports this project. It has three fields:
//     Kv: Primary data store, a key-Value map.
//     CountDiff: Keeps track of the number of times a value is currently set (count), relative to the parent (if parent exists).
//     Parent: This is used for transactions. Each time a new transaction is started, we create a new Store instance, with it's
//             parent set to the last Store instance. The new store only stores the values of the any keys that have been changed
//             in the current transaction (vs. storing an entire copy)
type Store struct {
	Kv        map[string]string
	CountDiff map[string]int
	Parent    *Store
}

// currentStore is the singelton pattern instance of the Store struct. It represents the current Store state
// that all actions are implemented upon.
var currentStore *Store

// InitStore initializes Store from blank state. This first needs to be called whenever this package is used.
func InitStore() {
	currentStore = NewStore(nil)

}

// GetCurrentStore provides the currently active Store instance. If the system is in a transaction, it provides the
// the intermediate transaction specfic store.
func GetCurrentStore() *Store {
	return currentStore
}

// SetCurrentStore sets the currently active store instance to the store instance provided as the param.
func SetCurrentStore(s *Store) {
	currentStore = s
}

// GetPrimaryStore provides the base Store, one that is above all the incomplete transactions.
func (s *Store) GetPrimaryStore() *Store {
	if s.Parent == nil {
		return s
	}
	return s.Parent.GetPrimaryStore()
}

// NewStore returns a newly initialized Store instance.
func NewStore(parent *Store) *Store {
	s := new(Store)
	s.Kv = make(map[string]string)
	s.CountDiff = make(map[string]int)
	s.Parent = parent
	return s
}

// Set sets the key to the provided value.
func (s *Store) Set(key, value string) error {
	// We could be overwritting an existing value, so in that case,
	// we should decrement the count for the old value.
	oldValue, err := s.Get(key)
	// any unexpected error should be returned
	if err != nil && err != ERR_KEY_NOT_EXIST {
		return err
	}
	// if old value exists, decrement it's count
	if err != ERR_KEY_NOT_EXIST {
		s.CountDiff[oldValue] -= 1
	}

	// set the new value, and increment it's count if not empty
	s.Kv[key] = value
	if value != "" {
		s.CountDiff[value] += 1
	}

	return nil
}

// Get provides the value set for key parameter. It returns ERR_KEY_NOT_EXIST error if the key
// has not been set up. Get is a recursive function when the system is currently in a transaction.
// It calls it's parent Stores to fetch the right value.
func (s *Store) Get(key string) (string, error) {
	value, exists := s.Kv[key]

	// Base Condition
	// If the key is set, return it. The first set value encountered is the most accurate for the current transaction
	if exists {
		return value, nil
	}
	// If we are not in a transaction, and the key is not set, return ERR_KEY_NOT_EXIST
	if s.Parent == nil {
		return value, ERR_KEY_NOT_EXIST
	}
	// Otherwise, if the key is not set in the currently active Store, recursively call the parent Store.
	return s.Parent.Get(key)
}

// Delete unsets the given key.
func (s *Store) Delete(key string) error {
	value, err := s.Get(key)
	if err != nil {
		return err
	}
	clog.Debugf("[DELETE] Key: %s | Original Value %s ", key, value)

	// Let's not delete the key, just set it to empty.
	// This means that when we're commiting a transaction, we know that a key has been deleted.
	s.Set(key, "") // same as deleting, since we're treating "" as NULL value
	return nil
}

// Count provides the number for times a value is actively set in the current state of the store. It
// recursively looks into in the parent Stores to calculate the count.
func (s *Store) Count(value string) (int, error) {
	var cnt int = s.CountDiff[value]
	// Base Condition
	// if we're not currently in a transaction, then current value in CountsDiff (cnt) is the final count value
	if s.Parent == nil {
		return cnt, nil
	}
	// Recursive: Otherwise, current value in CountsDiff is just diff (recent changes in counts) compared to the parent Store
	pCnt, err := s.Parent.Count(value)
	if err != nil {
		return -1, err
	}
	return cnt + pCnt, nil
}

// Begin begins a new transaction. From the system perspective, it means a new instance of Store struct,
// with its parent set to the currently active Store instance. Whenever a transaction begins, we create a
// new empty Store (not a copy) which can store intermediate states of those things that have changed.
func (s *Store) Begin() error {
	_s := NewStore(s)
	SetCurrentStore(_s)
	return nil
}

// Rollback cancels a transaction and undoes all the statements executed in the current transaction. It
// return ERR_NOT_TRANSACTION error if we're not in a transaction.
func (s *Store) Rollback() error {
	if s.Parent == nil {
		return ERR_NOT_TRANSACTION
	}
	SetCurrentStore(s.Parent)
	return nil
}

// Commit finalizes the current transaction, and makes transaction's Store state permanent. The implementation
// strategy does ensure concurrency safety since the already executed commands are not executed again on the real
// time updated primary store. It return ERR_NOT_TRANSACTION error if we're not in a transaction.
func (s *Store) Commit() error {
	if s.Parent == nil {
		return ERR_NOT_TRANSACTION
	}

	// We commit in two steps: 1) Update the KeyValue map of the primary Store, and 2) Update the CountDiff

	// Get the primary Store
	primary := s.GetPrimaryStore()

	// 1. Update the KeyValue map of the primary store based on the current transaction's Kv map (which
	// only contains keys/value that have been updated for the current transaction).
	// For any keys that we've set in the current transaction, update them in the primary store
	for k, v := range s.Kv {
		primary.Kv[k] = v
	}

	// 2. Update the CountDiff of the primary Store, by recursively taking count values and applying the diffs on the parent
	// all the way till the primary Store.
	primary.CountDiff = s.commitCountDiffs()

	// Replace the current store with the newly updated primary store
	SetCurrentStore(primary)

	return nil
}

// commitCountDiffs provides the counts of all the values stored in the current state of Store.
// It does that by recursively taking count values and applying the diffs on the parent
// all the way till the primary Store.
func (s *Store) commitCountDiffs() map[string]int {
	// Base Condition
	if s.Parent == nil {
		return s.CountDiff
	}
	for val, cnt := range s.CountDiff {
		s.Parent.CountDiff[val] += cnt
	}
	//Recursive
	return s.Parent.commitCountDiffs()
}
