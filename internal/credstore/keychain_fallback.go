package credstore

import "errors"

// ErrStoredRecordRemoved: replacing a Keychain item deleted the one stored
// before, and storing the new one then failed, so neither is stored.
var ErrStoredRecordRemoved = errors.New("the previously stored item was removed from the Keychain")

// OSStatus values (Security framework; keychain_write_darwin.go checks them
// against the SDK at compile time).
const (
	osInteractionNotAllowed = -25308 // errSecInteractionNotAllowed
	osNoAccessForItem       = -25243 // errSecNoAccessForItem
)

// keychainFallback reports whether an item update refused with status may
// be replaced by deleting the item and adding it again: only when the
// Keychain would not ask the person, or this build is not on the item's
// access list. errSecAuthFailed (the person clicked Deny, or a wrong
// keychain password) and errSecUserCanceled are the person's answer and
// never delete anything.
func keychainFallback(status int) bool {
	return status == osInteractionNotAllowed || status == osNoAccessForItem
}
