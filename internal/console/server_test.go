package console

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testToken = "test-token-0123456789"

func handler(t *testing.T, mutate func(*ServerOptions)) http.Handler {
	t.Helper()
	hosts, err := LoopbackHosts("127.0.0.1:8088")
	if err != nil {
		t.Fatal(err)
	}
	opt := ServerOptions{
		Console: New(Options{}), Mode: "demo", Token: testToken, Hosts: hosts,
		Submitter: &Submitter{}, // reaches the handler; presets are checked before any Redis call
	}
	if mutate != nil {
		mutate(&opt)
	}
	return Handler(opt)
}

func do(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1:8088"+path, strings.NewReader(body))
	for k, v := range headers {
		if k == "Host" {
			r.Host = v
			continue
		}
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

var goodWrite = map[string]string{"Content-Type": "application/json", "X-Console-Token": testToken, "Origin": "http://127.0.0.1:8088", "Sec-Fetch-Site": "same-origin"}

func with(base map[string]string, k, v string) map[string]string {
	out := map[string]string{}
	for kk, vv := range base {
		out[kk] = vv
	}
	if v == "" {
		delete(out, k)
	} else {
		out[k] = v
	}
	return out
}

func TestReadsNeedOnlyTheRightHost(t *testing.T) {
	h := handler(t, nil)
	if w := do(h, "GET", "/api/v1/status", "", nil); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	// DNS rebinding: a hostile page whose name now resolves to 127.0.0.1.
	if w := do(h, "GET", "/api/v1/status", "", map[string]string{"Host": "attacker.example:8088"}); w.Code != http.StatusMisdirectedRequest {
		t.Errorf("foreign Host = %d, want 421", w.Code)
	}
	if w := do(h, "GET", "/api/v1/nope", "", nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown endpoint = %d, want 404", w.Code)
	}
}

func TestWritesAreRefusedWithoutEveryCheck(t *testing.T) {
	h := handler(t, nil)
	body := `{"preset":"no-such-preset"}`
	cases := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"no token", with(goodWrite, "X-Console-Token", ""), http.StatusUnauthorized},
		{"wrong token", with(goodWrite, "X-Console-Token", "guess"), http.StatusUnauthorized},
		{"cross-origin", with(goodWrite, "Origin", "https://attacker.example"), http.StatusForbidden},
		{"cross-site fetch", with(goodWrite, "Sec-Fetch-Site", "cross-site"), http.StatusForbidden},
		{"form post", with(goodWrite, "Content-Type", "application/x-www-form-urlencoded"), http.StatusUnsupportedMediaType},
		{"foreign host", with(goodWrite, "Host", "attacker.example:8088"), http.StatusMisdirectedRequest},
	}
	for _, c := range cases {
		if w := do(h, "POST", "/api/v1/workload", body, c.headers); w.Code != c.want {
			t.Errorf("%s: %d, want %d (%s)", c.name, w.Code, c.want, w.Body)
		}
	}
	// With every check passed the request reaches the handler, which rejects
	// the preset itself: proof the guards above were what refused the others.
	if w := do(h, "POST", "/api/v1/workload", body, goodWrite); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "unknown preset") {
		t.Errorf("valid request: %d %s, want 400 unknown preset", w.Code, w.Body)
	}
}

func TestWritesRejectUnknownFieldsAndOversizedBodies(t *testing.T) {
	h := handler(t, nil)
	if w := do(h, "POST", "/api/v1/workload", `{"preset":"steady","count":1e9}`, goodWrite); w.Code != http.StatusBadRequest {
		t.Errorf("unknown field: %d, want 400", w.Code)
	}
	big := `{"preset":"` + strings.Repeat("x", 10_000) + `"}`
	if w := do(h, "POST", "/api/v1/workload", big, goodWrite); w.Code != http.StatusBadRequest {
		t.Errorf("oversized body: %d, want 400", w.Code)
	}
}

func TestFaultsAreRefusedUnlessEnabledInDemoMode(t *testing.T) {
	req := `{"action":"kill","target":"leader"}`
	off := handler(t, func(o *ServerOptions) { o.FaultsEnabled = false })
	if w := do(off, "POST", "/api/v1/faults", req, goodWrite); w.Code != http.StatusForbidden {
		t.Errorf("faults off: %d, want 403", w.Code)
	}
	// Enabled but with no supervisor: the console did not start these
	// processes, so it must not signal them.
	noSup := handler(t, func(o *ServerOptions) { o.FaultsEnabled = true; o.Supervisor = nil })
	if w := do(noSup, "POST", "/api/v1/faults", req, goodWrite); w.Code != http.StatusForbidden {
		t.Errorf("no supervisor: %d, want 403", w.Code)
	}
	monitor := handler(t, func(o *ServerOptions) { o.Mode = "monitor"; o.Token = ""; o.Submitter = nil })
	if w := do(monitor, "POST", "/api/v1/faults", req, with(goodWrite, "X-Console-Token", "")); w.Code != http.StatusForbidden {
		t.Errorf("monitor mode: %d, want 403", w.Code)
	}
}

func TestControlsSayWhyTheyAreUnavailable(t *testing.T) {
	h := handler(t, func(o *ServerOptions) { o.Mode = "monitor"; o.Submitter = nil })
	w := do(h, "GET", "/api/v1/controls", "", nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"faults_enabled":false`) || !strings.Contains(w.Body.String(), "monitor mode") {
		t.Errorf("controls in monitor mode: %d %s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), testToken) {
		t.Error("the token was served by the console")
	}
}

func TestStaticFilesStayInsideTheirDirectory(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "dist")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html>site</html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "secret.txt"), []byte("do-not-serve"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := handler(t, func(o *ServerOptions) { o.Static = dir })
	for _, p := range []string{"/", "/some/route"} {
		if w := do(h, "GET", p, "", nil); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "site") {
			t.Errorf("%s: %d %q, want the index", p, w.Code, w.Body.String())
		}
	}
	for _, p := range []string{"/../secret.txt", "/%2e%2e/secret.txt", "/..%2fsecret.txt", "/dist/../../secret.txt"} {
		if w := do(h, "GET", p, "", nil); strings.Contains(w.Body.String(), "do-not-serve") {
			t.Errorf("%s served a file outside the static directory", p)
		}
	}
	w := do(h, "GET", "/", "", nil)
	if w.Header().Get("Content-Security-Policy") == "" || w.Header().Get("X-Frame-Options") != "DENY" {
		t.Error("security headers missing")
	}
}
