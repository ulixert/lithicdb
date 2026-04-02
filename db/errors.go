package db

import "errors"

// ErrConflict is returned by Transaction.Commit when a write-write conflict
// is detected: another transaction (or direct write) modified a key that
// this transaction also wrote, after this transaction's snapshot was taken.
var ErrConflict = errors.New("theseon: transaction conflict")

// ErrTxClosed is returned when an operation is attempted on a transaction
// that has already been committed or rolled back.
var ErrTxClosed = errors.New("theseon: transaction already committed or rolled back")
