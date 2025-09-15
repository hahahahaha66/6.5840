package lock

import (
	"time"

	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	name string
	mark string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, name: l}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	key := "lock-" + lk.name
	for {
		values, version, _ := lk.ck.Get(key)
		if values != "" {
			time.Sleep(100)
		} else {
			lk.mark = kvtest.RandValue(8)
			err := lk.ck.Put(key, lk.mark, version)
			if err == "OK" {
				break
			} else if err == "ErrMaybe" {
				evalues, _, _ := lk.ck.Get(key)
				if lk.mark == evalues {
					break
				} else {
					continue
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	key := "lock-" + lk.name
	values, version, _ := lk.ck.Get(key)
	for values == lk.mark {
		err := lk.ck.Put(key, "", version)
		if err == "OK" {
			break
		} else if err == "ErrMaybe" {
			evalues, _, _ := lk.ck.Get(key)
			if evalues == "" {
				break
			} else {
				continue
			}
		}
	}
}
