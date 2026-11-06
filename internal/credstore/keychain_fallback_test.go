package credstore

import "testing"

func TestKeychainFallbackOnlyForAccessStatuses(t *testing.T) {
	for status, want := range map[int]bool{
		-25308: true,  // errSecInteractionNotAllowed
		-25243: true,  // errSecNoAccessForItem
		-25293: false, // errSecAuthFailed: Deny, or a wrong password
		-128:   false, // errSecUserCanceled
		-25300: false, // errSecItemNotFound (added, not replaced)
		-25299: false, // errSecDuplicateItem
		-34018: false, // errSecMissingEntitlement
		0:      false,
	} {
		if got := keychainFallback(status); got != want {
			t.Errorf("keychainFallback(%d) = %v, want %v", status, got, want)
		}
	}
}
