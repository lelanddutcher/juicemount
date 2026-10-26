package nativeclient

import "errors"

// RecoverProfile acknowledges only an existing strict profile. Inventory and
// record validation and initialization barriers share one control-lock owner.
// No reservation, copy identity, or profile component is created or replaced.
func (j *Journal) RecoverProfile() (resultErr error) {
	if j == nil || j.root == nil {
		return ErrJournalCorrupt
	}
	control, err := j.root.LockControl()
	if err != nil {
		return err
	}
	defer func() {
		if err := control.Close(); err != nil {
			resultErr = errors.Join(resultErr, ErrNativeExposureUncertain, err)
		}
	}()
	if _, _, err := j.scan(control); err != nil {
		return err
	}
	if err := control.SyncProfileInitialization(); err != nil {
		return errors.Join(ErrNativeExposureUncertain, err)
	}
	return nil
}
