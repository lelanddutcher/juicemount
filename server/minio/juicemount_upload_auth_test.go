// Copyright (c) 2026 JuiceMount contributors.
// SPDX-License-Identifier: AGPL-3.0-only

package cmd

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio/internal/auth"
	xhttp "github.com/minio/minio/internal/http"
)

type jmAuthReadCounter struct {
	io.Reader
	bytes int
}

func (r *jmAuthReadCounter) Read(p []byte) (int, error) {
	n, e := r.Reader.Read(p)
	r.bytes += n
	return n, e
}
func (r *jmAuthReadCounter) Close() error { return nil }

func jmAuthTar(t *testing.T, name string, payload []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	if err := w.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// This overlay tests the pinned upstream implementation through its production
// router. All state is created by the upstream isolated object-layer fixture.
// It is never compiled into a distribution binary.
func TestJuiceMountUnsignedTrailerAuth(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{
		t: t,
		// Establish globalObjectAPI with the existing fixture helper, then use the
		// complete production router so Snowball and UploadPart routing are covered.
		endpoints: []string{"GetObject"},
		objAPITest: func(obj ObjectLayer, instance, bucket string, _ http.Handler, cred auth.Credentials, t *testing.T) {
			router := initTestAPIEndPoints(obj, nil)
			for _, route := range []string{"put", "part", "snowball"} {
				for _, mode := range []string{"valid_header", "forged_header", "valid_query", "forged_query"} {
					t.Run(instance+"/"+route+"/"+mode, func(t *testing.T) {
						original := []byte("existing-owned-object-must-survive")
						attempted := []byte("forged-request-must-not-replace-it")
						key := "jm-auth-" + route + "-" + mode
						target := getPutObjectURL("http://localhost", bucket, key)
						uploadID := ""
						if route == "part" {
							info, err := obj.NewMultipartUpload(t.Context(), bucket, key, ObjectOptions{})
							if err != nil {
								t.Fatal(err)
							}
							uploadID = info.UploadID
							t.Cleanup(func() {
								ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
								defer cancel()
								_ = obj.AbortMultipartUpload(ctx, bucket, key, uploadID, ObjectOptions{})
							})
							target = getPutObjectPartURL("http://localhost", bucket, key, uploadID, "1")
						}
						if route == "snowball" {
							target = getPutObjectURL("http://localhost", bucket, "jm-auth-archive.tar")
						}
						serve := func(r *http.Request) *httptest.ResponseRecorder {
							t.Helper()
							r.RequestURI = r.URL.RequestURI()
							rec := httptest.NewRecorder()
							router.ServeHTTP(rec, r)
							return rec
						}
						signedPlain := func(payload []byte) *http.Request {
							t.Helper()
							if route == "snowball" {
								payload = jmAuthTar(t, key, payload)
							}
							headers := map[string]string{}
							if route == "snowball" {
								headers["X-Amz-Meta-Snowball-Auto-Extract"] = "true"
							}
							r, err := newTestSignedRequestV4(http.MethodPut, target, int64(len(payload)), bytes.NewReader(payload), cred.AccessKey, cred.SecretKey, headers)
							if err != nil {
								t.Fatal(err)
							}
							return r
						}
						// The seed uses ordinary signed payloads. Removing an unsigned-trailer
						// guard cannot invalidate this control or prevent the witness from running.
						seed := serve(signedPlain(original))
						if seed.Code != http.StatusOK {
							t.Fatalf("JM_AUTH_SEED: status=%d body=%s", seed.Code, seed.Body.String())
						}
						seedETag := strings.Trim(seed.Header().Get("ETag"), "\"")
						if route == "part" && seedETag == "" {
							t.Fatal("JM_AUTH_SEED: missing part ETag")
						}
						payload := attempted
						if mode == "valid_header" {
							payload = original
						}
						if route == "snowball" {
							payload = jmAuthTar(t, key, payload)
						}
						rawSnowball := route == "snowball" && mode == "forged_header"
						wire := payload
						if !rawSnowball {
							wire = []byte(fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n", len(payload), payload, crc32Checksum(payload)))
						}
						r, err := http.NewRequest(http.MethodPut, target, bytes.NewReader(wire))
						if err != nil {
							t.Fatal(err)
						}
						r.Header.Set(xhttp.AmzContentSha256, unsignedPayloadTrailer)
						r.Header.Set(xhttp.AmzDecodedContentLength, strconv.Itoa(len(payload)))
						if route == "snowball" {
							r.Header.Set("X-Amz-Meta-Snowball-Auto-Extract", "true")
						}
						if !rawSnowball {
							r.Header.Set("Content-Encoding", "aws-chunked")
							r.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
						}
						// The raw tar variant is the historical Snowball request: its forged
						// streaming marker must be rejected before tar extraction, even though
						// the attacker did not encode the body as AWS chunks.
						if strings.HasSuffix(mode, "query") {
							headers := []string{"host"}
							for h := range r.Header {
								if strings.HasPrefix(strings.ToLower(h), "x-amz-") {
									headers = append(headers, strings.ToLower(h))
								}
							}
							sort.Strings(headers)
							presignBoundaryRequest(t, r, UTCNow(), headers, cred)
							probe := r.Clone(t.Context())
							probe.Body = http.NoBody
							if code := reqSignatureV4Verify(probe, globalSite.Region(), serviceS3); code != ErrNone {
								t.Fatalf("JM_AUTH_QUERY_SEED: %v", niceError(code))
							}
							if mode == "forged_query" {
								q := r.URL.Query()
								q.Set(xhttp.AmzSignature, strings.Repeat("0", 64))
								r.URL.RawQuery = q.Encode()
								r.Form = q
							}
						} else {
							if err = signRequestV4(r, cred.AccessKey, cred.SecretKey); err != nil {
								t.Fatal(err)
							}
							if mode == "forged_header" {
								head, _, ok := strings.Cut(r.Header.Get(xhttp.Authorization), "Signature=")
								if !ok {
									t.Fatal("signature fixture missing")
								}
								r.Header.Set(xhttp.Authorization, head+"Signature="+strings.Repeat("0", 64))
							}
						}
						if got := getRequestAuthType(r); got != authTypeStreamingUnsignedTrailer {
							t.Fatalf("JM_AUTH_FIXTURE: wrong auth type %v", got)
						}
						counter := &jmAuthReadCounter{Reader: bytes.NewReader(wire)}
						r.Body = counter
						rec := serve(r)
						wantStatus, wantCode := http.StatusOK, ""
						if mode == "forged_header" {
							wantStatus, wantCode = http.StatusForbidden, "SignatureDoesNotMatch"
						}
						if strings.HasSuffix(mode, "query") {
							wantStatus, wantCode = http.StatusBadRequest, errorCodes.ToAPIErr(ErrSignatureVersionNotSupported).Code
						}
						var response APIErrorResponse
						if rec.Code != http.StatusOK {
							_ = xml.Unmarshal(rec.Body.Bytes(), &response)
						}
						if rec.Code != wantStatus || response.Code != wantCode {
							t.Errorf("JM_AUTH_UNEXPECTED_STATUS: got=%d code=%s want=%d code=%s", rec.Code, response.Code, wantStatus, wantCode)
						}
						if mode == "valid_header" {
							if counter.bytes == 0 {
								t.Error("JM_AUTH_ACTIVITY: valid upload consumed no body")
							}
						} else if counter.bytes != 0 {
							t.Errorf("JM_AUTH_BODY_READ: rejected authentication consumed %d bytes", counter.bytes)
						}
						if route == "part" {
							parts, err := obj.ListObjectParts(t.Context(), bucket, key, uploadID, 0, 1000, ObjectOptions{})
							if err != nil {
								t.Fatal(err)
							}
							wantETag := seedETag
							if mode == "valid_header" {
								wantETag = strings.Trim(rec.Header().Get("ETag"), "\"")
							}
							if wantETag == "" {
								t.Error("JM_AUTH_PART_ETAG: missing successful upload ETag")
							}
							if len(parts.Parts) != 1 || parts.Parts[0].ETag != wantETag || parts.Parts[0].Size != int64(len(original)) {
								t.Errorf("JM_AUTH_PART_CHANGED: stored part differs from seed: %+v", parts.Parts)
							}
							// Use the actual surviving part to complete, then check canonical bytes.
							if len(parts.Parts) != 1 {
								t.Fatal("part count prevents canonical readback")
							}
							_, err = obj.CompleteMultipartUpload(t.Context(), bucket, key, uploadID, []CompletePart{{PartNumber: 1, ETag: parts.Parts[0].ETag}}, ObjectOptions{})
							if err != nil {
								t.Fatal(err)
							}
						}
						get, err := newTestSignedRequestV4(http.MethodGet, getGetObjectURL("http://localhost", bucket, key), 0, nil, cred.AccessKey, cred.SecretKey, nil)
						if err != nil {
							t.Fatal(err)
						}
						readback := serve(get)
						if mode != "valid_header" && rec.Code == http.StatusOK && readback.Code == http.StatusOK && bytes.Equal(readback.Body.Bytes(), attempted) {
							t.Log("JM_AUTH_BYPASS_WITNESS: write=200 read=200 exact_attempted_bytes=true")
						}
						if readback.Code != http.StatusOK || !bytes.Equal(readback.Body.Bytes(), original) {
							t.Errorf("JM_AUTH_READBACK_CHANGED: status=%d got=%q want=%q", readback.Code, readback.Body.Bytes(), original)
						}
					})
				}
			}
		},
	})
}
