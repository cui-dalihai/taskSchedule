package common

import "errors"

var (
	ERR_LOCK_ALREADY_IN_USED = errors.New("锁已经被占用")
)
