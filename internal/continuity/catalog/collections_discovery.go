package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

// QueryCollections emits one completely encoded, freshly fenced page. Callers
// own output cancellation; the service introduces no worker or background I/O.
func (d *DiscoveryService) QueryCollections(parent context.Context, bearer []byte, input CollectionQueryRequest, output io.Writer) (result DisclosureResult, resultErr error) {
	if d == nil || output == nil {
		return result, invalid("collection read service or writer")
	}
	ctx, leave, err := d.admit(parent)
	if err != nil {
		return result, err
	}
	defer leave()
	request, err := ValidateCollectionQueryRequest(input, d.library)
	if err != nil {
		return result, err
	}
	if !supportsCollections(d.store.version) {
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
	handle, c, subject, err := d.store.authenticateSnapshot(ctx, tx, d.library, bearer)
	if err != nil {
		return result, err
	}
	result.SubjectID, result.Selector = c.subject, c.selector
	if err := collectionReadEligible(c, subject); err != nil {
		return result, err
	}
	auth := discoveryBinding{handle, subject.AuthorizationGeneration, c.issued, c.expires, nil}
	var reservation int64
	var buildingSubject bool
	var window *discoveryWindow
	var page *collectionReadPage
	var bodyPinned, pageBuilding bool
	// This call owns every reservation/lease exactly once, including errors before
	// publication and Close cancellation during construction or external writing.
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.bytes -= reservation
		if buildingSubject {
			d.releaseBuildingLocked(c.subject)
		}
		if window != nil {
			if pageBuilding {
				page.building = false
			}
			if bodyPinned {
				page.leases--
			}
			window.leases--
			if window.retired && window.leases == 0 {
				delete(d.windows, window.ordinal)
				d.bytes -= window.charge
			}
		}
	}()
	var body []byte
	var binding collectionReadBinding
	var assetBindings []collectionAssetBinding
	if request.Kind == "page" {
		window, page, reservation, err = d.beginCollectionPage(ctx, request.Token, auth)
		if err != nil {
			return result, err
		}
		binding = window.collection.binding
		if page.body != nil {
			bodyPinned = true
			assetBindings = page.assets
			body = append([]byte(nil), page.body...)
		} else {
			pageBuilding = true
			rights, err := readCollectionReadRights(ctx, tx, c, binding.request.CollectionID)
			if err != nil {
				return result, err
			}
			bits := rights.collections[binding.request.CollectionID]
			def, err := readCollectionReadDefinition(ctx, tx, binding.request.CollectionID)
			if err != nil {
				return result, err
			}
			if bits&collectionReadBit == 0 || def.version != binding.definition.version || def.generation != binding.definition.generation {
				d.retireCollectionWindow(window)
				return result, errDiscoveryRestart
			}
			oldRights, err := readDiscoveryRights(ctx, tx, d.library, c)
			if err != nil {
				return result, err
			}
			items, assets, end, err := readCollectionMemberPage(ctx, tx, d.library, window.collection.members, page.start, binding.request.PageSize, oldRights)
			if err != nil {
				return result, err
			}
			var next *string
			nextToken := ""
			if end < len(window.collection.members) {
				nextToken, err = d.collectionToken()
				if err != nil {
					return result, err
				}
				next = &nextToken
			}
			encoded, err := encodeCollectionPage(window.collection, items, next)
			if err != nil {
				return result, err
			}
			if err := finish(); err != nil {
				return result, err
			}
			if err := d.publishCollectionPage(ctx, window, page, encoded, assets, end, nextToken); err != nil {
				return result, err
			}
			reservation -= discoveryBuildBytes
			pageBuilding = false
			bodyPinned = true
			body = append([]byte(nil), encoded...)
			assetBindings = assets
		}
	} else {
		if request.Kind == "open" {
			reservation = discoveryWorkBytes + discoveryCopyBytes
			d.mu.Lock()
			if d.bytes+reservation > discoveryQuota {
				reservation = 0
				d.mu.Unlock()
				return result, errDiscoveryBusy
			}
			d.bytes += reservation
			d.mu.Unlock()
		} else {
			if err := d.reserveNew(auth); err != nil {
				return result, err
			}
			reservation = discoveryNewBytes
			buildingSubject = true
		}
		rights, err := readCollectionReadRights(ctx, tx, c, request.CollectionID)
		if err != nil {
			return result, err
		}
		binding = collectionReadBinding{auth: auth, request: request}
		var summary CollectionSummary
		var items []json.RawMessage
		var ids []collectionMemberIdentity
		end := 0
		complete := true
		if request.Kind == "directory" {
			var bindings []collectionDirectoryBinding
			var canCreate bool
			items, bindings, canCreate, err = readCollectionDirectory(ctx, tx, c, request, rights)
			if err != nil {
				return result, err
			}
			binding.directory = bindings
			binding.canCreate = canCreate
		} else {
			bits := rights.collections[request.CollectionID]
			required := collectionReadBit
			if request.Kind == "access" {
				required |= collectionEditBit | collectionAdminBit
			}
			if bits&required != required {
				return result, ErrNotFound
			}
			def, err := readCollectionReadDefinition(ctx, tx, request.CollectionID)
			if err != nil {
				return result, err
			}
			binding.definition = def
			summary = collectionSummary(def, c, bits)
			switch request.Kind {
			case "open":
				body, err = encodeDiscoveryValue(CollectionOpenResponse{1, "collection", summary}, collectionEnvelopeBytes)
			case "access":
				items, err = readCollectionAccess(ctx, tx, request.CollectionID)
			case "members", "history":
				oldRights, e := readDiscoveryRights(ctx, tx, d.library, c)
				if e != nil {
					return result, e
				}
				if request.Kind == "members" {
					ids, err = readCollectionMembers(ctx, tx, request, oldRights)
					if err == nil {
						items, assetBindings, end, err = readCollectionMemberPage(ctx, tx, d.library, ids, 0, request.PageSize, oldRights)
					}
				} else {
					items, complete, err = readCollectionReadHistory(ctx, tx, d.library, c, request, oldRights)
				}
			}
			if err != nil {
				return result, err
			}
		}
		if request.Kind != "open" {
			built, err := d.makeCollectionWindow(binding, summary, ids, items, assetBindings, end, complete)
			if err != nil {
				return result, err
			}
			// Serialization and all construction reads precede cleanup and final fence.
			if err := finish(); err != nil {
				return result, err
			}
			if err := d.publishCollectionWindow(ctx, built); err != nil {
				return result, err
			}
			reservation -= discoveryBuildBytes
			buildingSubject = false
			window = built
			page = built.collection.pages[0]
			bodyPinned = true
			body = append([]byte(nil), page.body...)
		}
	}
	if done != nil {
		if err := finish(); err != nil {
			d.retireCollectionWindow(window)
			return result, err
		}
	}
	<-d.sql
	sqlHeld = false
	if d.beforeFence != nil {
		if err := d.beforeFence(ctx); err != nil {
			d.retireCollectionWindow(window)
			return result, err
		}
	}
	if err := d.store.checkPaths(); err != nil {
		d.retireCollectionWindow(window)
		return result, err
	}
	select {
	case d.sql <- struct{}{}:
		sqlHeld = true
	default:
		d.retireCollectionWindow(window)
		return result, errDiscoveryBusy
	}
	if err := d.collectionReadFence(ctx, binding, assetBindings); err != nil {
		d.retireCollectionWindow(window)
		return result, err
	}
	if err := d.store.checkPaths(); err != nil {
		d.retireCollectionWindow(window)
		return result, err
	}
	<-d.sql
	sqlHeld = false
	// No queue, encoding, SQL or deferred owner cleanup between this check and
	// the first external write. Published retries keep their original bytes.
	now := d.store.now().Unix()
	if now < binding.auth.issued || now >= binding.auth.expires {
		d.retireCollectionWindow(window)
		return result, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		d.retireCollectionWindow(window)
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
