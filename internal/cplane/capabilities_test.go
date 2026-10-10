package cplane

import (
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestContractVersionMatchesVendoredContract(t *testing.T) {
	version, err := os.ReadFile("../../contract/VERSION")
	if err != nil {
		t.Fatal(err)
	}
	want, err := strconv.Atoi(strings.TrimSpace(string(version)))
	if err != nil {
		t.Fatal(err)
	}
	if ContractVersion != want {
		t.Fatalf("provider contract version = %d, vendored VERSION = %d", ContractVersion, want)
	}
	for _, deployment := range []string{"gui", "cli"} {
		body, err := os.ReadFile("../../contract/fixtures/whoami/" + deployment + ".json")
		if err != nil {
			t.Fatal(err)
		}
		var fixture struct {
			ContractVersion int `json:"contract_version"`
		}
		if err := json.Unmarshal(body, &fixture); err != nil {
			t.Fatal(err)
		}
		if fixture.ContractVersion != want {
			t.Errorf("%s fixture contract version = %d, vendored VERSION = %d", deployment, fixture.ContractVersion, want)
		}
	}
}

// REGISTER-ROUTE (2026-08-04). A consumer cannot tell "AI-only contribute" from
// "widened contribute" by looking at `contribute` — an OLD provider advertises
// that exact string. The distinct token is the whole feature-detection story, so
// pin that the register route emits BOTH.
func TestRegisterRouteAdvertisesBothContributeTokens(t *testing.T) {
	caps := DeriveCapabilities([]string{"/derivatives/register"})

	for _, want := range []string{"contribute", "contribute-derivatives"} {
		if !slices.Contains(caps, want) {
			t.Errorf("serving /derivatives/register must advertise %q; got %v", want, caps)
		}
	}

	// A binary that does NOT serve the route must advertise neither — the token
	// has to mean "this route is here", not "this build was compiled recently".
	bare := DeriveCapabilities([]string{"/health", "/metrics"})
	for _, unwanted := range []string{"contribute", "contribute-derivatives"} {
		if slices.Contains(bare, unwanted) {
			t.Errorf("a binary not serving the register route must not advertise %q; got %v", unwanted, bare)
		}
	}
}

// Every token DeriveCapabilities can emit must be in the closed vocabulary,
// otherwise /whoami fails its own schema enum.
func TestAllAliasTokensAreInTheVocabulary(t *testing.T) {
	for route, tokens := range routeCapAlias {
		for _, tok := range tokens {
			if !capabilityVocab[tok] {
				t.Errorf("route %q aliases to %q which is NOT in capabilityVocab — /whoami would fail its enum", route, tok)
			}
		}
	}
}

// JM-22. A route the consumer must FEATURE-DETECT is worthless if serving it
// does not advertise it: their only other signal is a 404 from an older
// provider, which is indistinguishable from a transient failure.
//
// This test exists because the alias was added WITHOUT one, and dropping the
// alias entirely left the whole suite green — the advertisement was
// structurally untested.
func TestBatchRouteAdvertisesItsToken(t *testing.T) {
	caps := DeriveCapabilities([]string{"/derivatives/batch"})
	if !slices.Contains(caps, "derivatives-batch") {
		t.Errorf("serving /derivatives/batch must advertise \"derivatives-batch\" so the "+
			"consumer can feature-detect instead of probing for a 404; got %v", caps)
	}

	bare := DeriveCapabilities([]string{"/health", "/derivatives"})
	if slices.Contains(bare, "derivatives-batch") {
		t.Errorf("a binary NOT serving /derivatives/batch must not advertise the token — "+
			"a consumer would send a 1000-inode POST and get a 404; got %v", bare)
	}
}

// B-7. capabilityVocab is the closed set DeriveCapabilities may emit; the
// vendored whoami schema enum is what consumers validate against. They must be
// the same set: a vocab-only token is advertised and then rejected by the
// consumer's schema, and an enum-only token is a capability this provider can
// never advertise.
func TestVocabularyMatchesVendoredWhoamiSchemaEnum(t *testing.T) {
	body, err := os.ReadFile("../../contract/spec/schema/whoami.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Capabilities struct {
				Items struct {
					Enum []string `json:"enum"`
				} `json:"items"`
			} `json:"capabilities"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(body, &schema); err != nil {
		t.Fatal(err)
	}
	enum := map[string]bool{}
	for _, tok := range schema.Properties.Capabilities.Items.Enum {
		enum[tok] = true
	}
	if len(enum) == 0 {
		t.Fatal("vendored whoami schema has no capability enum; refusing to report")
	}
	for tok := range capabilityVocab {
		if !enum[tok] {
			t.Errorf("capabilityVocab token %q is NOT in the vendored whoami schema enum — /whoami would fail its own schema", tok)
		}
	}
	for tok := range enum {
		if !capabilityVocab[tok] {
			t.Errorf("vendored whoami schema enum token %q is missing from capabilityVocab", tok)
		}
	}
}
