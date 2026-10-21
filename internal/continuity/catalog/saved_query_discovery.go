package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

// QuerySavedQueries writes one complete canonical page after the current-rights
// fence. It owns the original snapshot, pool reservation and writer until return.
func (d *DiscoveryService) QuerySavedQueries(ctx context.Context, bearer []byte, input SavedQueryReadRequest, output io.Writer) (DisclosureResult, error) {
	if d == nil || output == nil {
		return DisclosureResult{}, invalid("saved query service or writer")
	}
	r, err := ValidateSavedQueryReadRequest(input, d.library)
	if err != nil {
		return DisclosureResult{}, err
	}
	return d.querySavedRead(ctx, bearer, r, savedQueryReadFamily, output)
}
func (d *DiscoveryService) QueryRecents(ctx context.Context, bearer []byte, input RecentsReadRequest, output io.Writer) (DisclosureResult, error) {
	if d == nil || output == nil {
		return DisclosureResult{}, invalid("recents service or writer")
	}
	r, err := ValidateRecentsReadRequest(input, d.library)
	if err != nil {
		return DisclosureResult{}, err
	}
	s := SavedQueryReadRequest{Version: r.Version, Kind: r.Kind, LibraryID: r.LibraryID, PageSize: r.PageSize, Token: r.Token}
	if r.Kind == "page" {
		s.QueryKind = "list"
	}
	return d.querySavedRead(ctx, bearer, s, recentsReadFamily, output)
}

func (d *DiscoveryService) querySavedRead(parent context.Context, bearer []byte, r SavedQueryReadRequest, family string, output io.Writer) (result DisclosureResult, resultErr error) {
	ctx, leave, err := d.admit(parent)
	if err != nil {
		return result, err
	}
	defer leave()
	if !supportsSavedQueries(d.store.version) {
		return result, ErrUpgradeRequired
	}
	if err := d.store.enter(); err != nil {
		return result, err
	}
	defer d.store.leave()
	select {
	case d.sql <- struct{}{}:
	default:
		return result, errDiscoveryBusy
	}
	sqlHeld := true
	defer func() {
		if sqlHeld {
			<-d.sql
		}
	}()
	tx, done, err := d.store.readSnapshot(ctx)
	if err != nil {
		return result, err
	}
	defer func() {
		if done != nil {
			resultErr = errors.Join(resultErr, d.finishSnapshot(done))
		}
	}()
	finish := func() error { f := done; done = nil; return d.finishSnapshot(f) }
	handle, c, sub, err := d.store.authenticateSnapshot(ctx, tx, d.library, bearer)
	if err != nil {
		return result, err
	}
	result.SubjectID, result.Selector = c.subject, c.selector
	if family == recentsReadFamily {
		err = recentsReadEligible(c, sub)
	} else {
		err = collectionReadEligible(c, sub)
	}
	if err != nil {
		return result, err
	}
	auth := discoveryBinding{handle, sub.AuthorizationGeneration, c.issued, c.expires, nil}
	var reservation int64
	building := false
	var window *discoveryWindow
	defer func() {
		if done != nil {
			resultErr = errors.Join(resultErr, finish())
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		d.bytes -= reservation
		if building {
			d.releaseBuildingLocked(c.subject)
		}
		if window != nil {
			if resultErr != nil {
				d.retireLocked(window)
			}
			window.leases--
			if window.retired && window.leases == 0 {
				delete(d.windows, window.ordinal)
				d.bytes -= window.charge
			}
		}
	}()
	var body []byte
	var binding savedQueryReadBinding
	if r.Kind == "page" {
		var index int
		window, index, reservation, err = d.beginSavedReadPage(ctx, r, family, auth)
		if err != nil {
			return result, err
		}
		if family == recentsReadFamily {
			binding = window.recent.binding
		} else {
			binding = window.saved.binding
		}
		binding.auth = cloneDiscoveryBinding(binding.auth)
		// Close the original authentication snapshot before serialization/fencing.
		if err := finish(); err != nil {
			return result, err
		}
		if family == recentsReadFamily {
			body, err = serializeRecentsPage(window, index)
		} else {
			body = exactReadBytes(window.bodies[index])
		}
		if err != nil {
			return result, err
		}
	} else {
		direct := family == savedQueryReadFamily && (r.Kind == "open" || r.Kind == "definition")
		if direct {
			reservation = savedReadActiveBytes
			d.mu.Lock()
			if d.bytes+reservation > discoveryQuota {
				reservation = 0
				d.mu.Unlock()
				return result, errDiscoveryBusy
			}
			d.bytes += reservation
			d.mu.Unlock()
		} else {
			if err := d.reserveSavedReadNew(auth); err != nil {
				return result, err
			}
			reservation = discoveryNewBytes
			building = true
		}
		binding = savedQueryReadBinding{auth: auth, request: r}
		binding.metadata, err = readMetadataEpoch(ctx, tx)
		if err != nil {
			return result, err
		}
		var built *discoveryWindow
		if family == recentsReadFamily {
			request, err := recentsReadBytes(RecentsReadRequest{Version: r.Version, Kind: r.Kind, LibraryID: r.LibraryID, PageSize: r.PageSize})
			if err != nil {
				return result, err
			}
			built, err = d.readRecentsWindow(ctx, tx, c, binding, r.PageSize, request)
			if err != nil {
				return result, err
			}
			binding = built.recent.binding
		} else {
			rights, err := readCollectionReadRights(ctx, tx, c, r.CollectionID)
			if err != nil {
				return result, err
			}
			w := &savedQueryReadWindow{binding: binding, complete: true}
			request, err := savedQueryReadBytes(r)
			if err != nil {
				return result, err
			}
			if r.Kind == "directory" {
				w.items, w.binding.directory, w.binding.canCreate, err = readSavedQueryDirectory(ctx, tx, c, r, rights, true)
				if err != nil {
					return result, err
				}
			} else {
				bits := rights.collections[r.CollectionID]
				required := collectionReadBit
				if r.Kind == "access" {
					required |= collectionEditBit | collectionAdminBit
				}
				if bits&required != required {
					return result, ErrNotFound
				}
				def, err := readSavedQueryDefinition(ctx, tx, r.CollectionID)
				if err != nil {
					return result, err
				}
				w.summary = savedReadSummary(def, c, bits)
				w.binding.version = def.version
				w.binding.policy = def.generation
				var assetRights discoveryRights
				if r.Kind == "definition" || r.Kind == "execute" || r.Kind == "history" {
					assetRights, err = readDiscoveryRights(ctx, tx, d.library, c)
					if err != nil {
						return result, err
					}
					if err := authorizeSavedQueryAnchors(ctx, tx, d.library, def.definition, assetRights); err != nil {
						return result, err
					}
				}
				switch r.Kind {
				case "open":
					body, err = encodeDiscoveryValue(SavedQueryOpenResponse{2, "collection", w.summary}, 1<<20)
				case "definition":
					body, err = encodeDiscoveryValue(SavedQueryDefinitionResponse{2, "definition", w.summary, def.definition}, 1<<20)
				case "access":
					old, e := readCollectionAccess(ctx, tx, r.CollectionID)
					err = e
					if err == nil {
						w.items = make([]json.RawMessage, len(old), 128)
						for i, item := range old {
							w.items[i] = exactReadBytes(item)
							old[i] = nil
						}
					}
				case "history":
					w.definition = exactReadBytes(def.canonical)
					w.items, w.complete, err = readSavedQueryHistory(ctx, tx, d.library, c, w, assetRights, len(request))
				case "execute":
					if def.archived {
						return result, ErrNotFound
					}
					w.definition = exactReadBytes(def.canonical)
					view, pref, e := d.readPreferenceView(ctx, tx, c.subject)
					if e != nil {
						return result, e
					}
					w.binding.auth.preference = pref
					w.items, w.complete, err = readSavedQueryExecution(ctx, tx, d.library, def, assetRights, view, pref, r.PageSize)
				}
				if err != nil {
					return result, err
				}
			}
			binding = w.binding
			if !direct {
				built, err = d.makeSavedReadWindow(w, request)
				if err != nil {
					return result, err
				}
			}
		}
		if err := finish(); err != nil {
			return result, err
		}
		if built != nil {
			active, err := d.publishSavedReadWindow(ctx, built)
			if err != nil {
				return result, err
			}
			reservation = active
			building = false
			window = built
			if family == recentsReadFamily {
				body, err = serializeRecentsPage(window, 0)
			} else {
				body = exactReadBytes(window.bodies[0])
			}
			if err != nil {
				return result, err
			}
		}
	}
	<-d.sql
	sqlHeld = false
	if d.beforeFence != nil {
		if err := d.beforeFence(ctx); err != nil {
			return result, err
		}
	}
	if err := d.store.checkPaths(); err != nil {
		return result, err
	}
	select {
	case d.sql <- struct{}{}:
		sqlHeld = true
	default:
		return result, errDiscoveryBusy
	}
	if err := d.savedReadFence(ctx, &binding, family); err != nil {
		return result, err
	}
	if err := d.store.checkPaths(); err != nil {
		return result, err
	}
	<-d.sql
	sqlHeld = false
	// No SQL, queueing, serialization or cleanup separates this check and Write.
	now := d.store.now().Unix()
	if now < binding.auth.issued || now >= binding.auth.expires {
		return result, ErrUnauthenticated
	}
	if binding.auth.preference != nil {
		if err := binding.auth.preference.checkTime(now); err != nil {
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.Started = true
	n, err := output.Write(body)
	if err != nil {
		return result, err
	}
	if n != len(body) {
		return result, io.ErrShortWrite
	}
	return result, nil
}
