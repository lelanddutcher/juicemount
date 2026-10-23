package mutationgateway

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

// Connection reuse does not extend the verified chain's validity interval.
func imageIdentityCurrent(r *http.Request) error {
	if _, err := identity(r); err != nil {
		return err
	}
	now := time.Now()
	for _, chain := range r.TLS.VerifiedChains {
		valid := len(chain) > 0
		for _, cert := range chain {
			if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
				valid = false
				break
			}
		}
		if valid {
			return nil
		}
	}
	return ErrUnauthorized
}
func decodeImage(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, mutationauthority.MaxImageEnvelope)
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return mutationauthority.ErrInvalid
	}
	return mutationauthority.DecodeImageCanonical(b, v)
}
func writeImageJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(imageJSON(v))
}
func failImage(w http.ResponseWriter, err error) {
	code, msg := 0, ""
	switch {
	case errors.Is(err, mutationauthority.ErrImageInactive):
		code, msg = http.StatusServiceUnavailable, "image_inactive"
	case errors.Is(err, mutationauthority.ErrImageQuota):
		code, msg = http.StatusConflict, "image_quota"
	case errors.Is(err, mutationauthority.ErrImageConflict):
		code, msg = http.StatusConflict, "image_generation_changed"
	case errors.Is(err, mutationauthority.ErrInFlight):
		code, msg = http.StatusConflict, "image_verification_in_flight"
	}
	if code == 0 {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(imageJSON(struct {
		Error string `json:"error"`
	}{msg}))
}
func (g *Gateway) serveImageHTTP(w http.ResponseWriter, r *http.Request, principal string) {
	current := func() error { return imageIdentityCurrent(r) }
	if err := current(); err != nil {
		failImage(w, err)
		return
	}
	switch r.URL.Path {
	case "/v1/images/seal":
		var seal ImageSeal
		if err := decodeImage(w, r, &seal); err != nil {
			failImage(w, err)
			return
		}
		receipt, err := g.sealImage(r.Context(), principal, seal, current)
		if err != nil {
			failImage(w, err)
			return
		}
		writeImageJSON(w, receipt)
	case "/v1/images/seal-status":
		var ref ImageRef
		if err := decodeImage(w, r, &ref); err != nil {
			failImage(w, err)
			return
		}
		record, err := g.journal.InspectImage(r.Context(), ref)
		if errors.Is(err, mutationauthority.ErrNoSuchOperation) {
			err = ErrUnauthorized
		}
		if err != nil {
			failImage(w, err)
			return
		}
		if record.Seal.Producer != principal {
			failImage(w, ErrUnauthorized)
			return
		}
		if err = current(); err != nil {
			failImage(w, err)
			return
		}
		writeImageJSON(w, ImageSealStatus{1, ref.StoreID, ref.ImageID, record.Digest, record.State, record.Receipt})
	case "/v1/images/status":
		var ref ImageRef
		if err := decodeImage(w, r, &ref); err != nil {
			failImage(w, err)
			return
		}
		view, err := g.imageStatus(r.Context(), principal, ref)
		if err != nil {
			failImage(w, err)
			return
		}
		if err = current(); err != nil {
			failImage(w, err)
			return
		}
		writeImageJSON(w, view)
	case "/v1/images/read":
		var request ImageRead
		if err := decodeImage(w, r, &request); err != nil {
			failImage(w, err)
			return
		}
		data, view, err := g.imageRead(r.Context(), principal, request, current)
		if err != nil {
			failImage(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("X-JM-Image-Digest", view.ImageDigest)
		w.Header().Set("X-JM-Image-Activation", strconv.FormatInt(view.Activation, 10))
		w.Header().Set("X-JM-Image-Generation", strconv.FormatInt(view.Generation, 10))
		w.Header().Set("X-JM-Image-Index", strconv.FormatInt(request.Index, 10))
		w.Header().Set("X-JM-Chunk-SHA256", view.Chunks[request.Index].SHA256)
		_, _ = w.Write(data)
	case "/v1/images/reader":
		var request ImageReaderChange
		if err := decodeImage(w, r, &request); err != nil {
			failImage(w, err)
			return
		}
		if _, _, err := request.Canonical(); err != nil {
			failImage(w, err)
			return
		}
		if _, err := g.journal.ImageForProducer(r.Context(), request.StoreID, request.ImageID, principal); err != nil {
			failImage(w, ErrUnauthorized)
			return
		}
		if err := current(); err != nil {
			failImage(w, err)
			return
		}
		receipt, err := g.journal.RebindImageReader(r.Context(), principal, request)
		if err != nil {
			failImage(w, err)
			return
		}
		writeImageJSON(w, receipt)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}
