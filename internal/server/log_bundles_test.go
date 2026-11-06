package server

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
)

func gz(t *testing.T, text string) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, _ = zw.Write([]byte(text))
	_ = zw.Close()
	return b.Bytes()
}

func chunkMsg(id string, seq int, last bool, part []byte) json.RawMessage {
	b, _ := json.Marshal(map[string]interface{}{
		"request_id": id, "seq": seq, "last": last, "data": base64.StdEncoding.EncodeToString(part),
	})
	return b
}

func TestBundleChunksAreStoredAndRead(t *testing.T) {
	b := newBundleManager(t.TempDir(), 30*24*time.Hour, logging.New("error"))
	now := time.Now()
	req := b.start("u-1", 1, now)
	data := gz(t, "line one\nERROR capture lost\nline three\n")
	half := len(data) / 2
	if err := b.chunk("u-1", chunkMsg(req.ID, 0, false, data[:half]), now); err != nil {
		t.Fatalf("chunk 0: %v", err)
	}
	if err := b.chunk("u-1", chunkMsg(req.ID, 1, true, data[half:]), now); err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	files, reqs := b.list("u-1", now)
	if len(files) != 1 || files[0].RequestID != req.ID || files[0].Bytes != int64(len(data)) {
		t.Fatalf("files = %+v", files)
	}
	if len(reqs) != 1 || reqs[0].Status != "stored" {
		t.Fatalf("requests = %+v", reqs)
	}
	path, ok := b.path("u-1", files[0].Name)
	if !ok {
		t.Fatal("no path for the stored bundle")
	}
	lines, matched, total, err := b.view(path, "error", 100)
	if err != nil || matched != 1 || total != 3 || lines[0] != "ERROR capture lost" {
		t.Fatalf("view = %v %d %d %v", lines, matched, total, err)
	}
}

func TestBundleChunksAreRefused(t *testing.T) {
	b := newBundleManager(t.TempDir(), 30*24*time.Hour, logging.New("error"))
	now := time.Now()
	data := gz(t, "x\n")

	other := b.start("u-1", 1, now)
	if err := b.chunk("u-2", chunkMsg(other.ID, 0, true, data), now); err == nil {
		t.Fatal("another user's socket filled the request")
	}
	order := b.start("u-1", 1, now)
	if err := b.chunk("u-1", chunkMsg(order.ID, 1, true, data), now); err == nil {
		t.Fatal("a chunk out of order was taken")
	}
	big := b.start("u-1", 1, now)
	part := make([]byte, bundleChunkBytes)
	var err error
	for seq := 0; err == nil && seq < bundleMaxBytes/bundleChunkBytes+2; seq++ {
		err = b.chunk("u-1", chunkMsg(big.ID, seq, false, part), now)
	}
	if err == nil || !strings.Contains(err.Error(), "over") {
		t.Fatalf("a bundle over the cap was taken: %v", err)
	}
	notGzip := b.start("u-1", 1, now)
	if err := b.chunk("u-1", chunkMsg(notGzip.ID, 0, true, []byte("plain")), now); err == nil {
		t.Fatal("a bundle that is not gzip was stored")
	}
	clientErr := b.start("u-1", 1, now)
	msg, _ := json.Marshal(map[string]interface{}{"request_id": clientErr.ID, "error": "no log file"})
	if err := b.chunk("u-1", msg, now); err == nil {
		t.Fatal("a client error is not a failure")
	}
	if err := b.chunk("u-1", chunkMsg("not-an-id", 0, true, data), now); err == nil {
		t.Fatal("a bad request id was taken")
	}
	files, reqs := b.list("u-1", now)
	if len(files) != 0 {
		t.Fatalf("refused chunks stored files: %+v", files)
	}
	failed := 0
	for _, r := range reqs {
		if r.Status == "failed" {
			failed++
		}
	}
	if failed != 4 {
		t.Fatalf("failed requests = %d, want 4: %+v", failed, reqs)
	}
}

func TestBundleRequestsExpireAndBundlesAreSwept(t *testing.T) {
	dir := t.TempDir()
	b := newBundleManager(dir, 30*24*time.Hour, logging.New("error"))
	past := time.Now().Add(-11 * time.Minute)
	req := b.start("u-1", 1, past)
	_, reqs := b.list("u-1", time.Now())
	if len(reqs) != 1 || reqs[0].Status != "expired" {
		t.Fatalf("an unanswered request after 10 min = %+v", reqs)
	}
	if err := b.chunk("u-1", chunkMsg(req.ID, 0, true, gz(t, "late\n")), time.Now()); err == nil {
		t.Fatal("a chunk for an expired request was taken")
	}
	old := time.Now().Add(-31 * 24 * time.Hour)
	if err := os.MkdirAll(b.userDir("u-1"), 0o755); err != nil {
		t.Fatal(err)
	}
	oldName := filepathName(old, req.ID)
	if err := os.WriteFile(filepath.Join(b.userDir("u-1"), oldName), gz(t, "old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	b.sweep(time.Now())
	if files, _ := b.list("u-1", time.Now()); len(files) != 0 {
		t.Fatalf("an old bundle was kept: %+v", files)
	}
}

func filepathName(at time.Time, id string) string {
	return strings.Replace("0000000000000-"+id+".log.gz", "0000000000000", padMs(at.UnixMilli()), 1)
}

func padMs(ms int64) string {
	s := itoa64(ms)
	for len(s) < 13 {
		s = "0" + s
	}
	return s
}

func TestBundleUserDirsStayInTheRoot(t *testing.T) {
	b := newBundleManager(t.TempDir(), time.Hour, logging.New("error"))
	for _, user := range []string{"../../etc", "a/b", "..", ".hidden", "naïve"} {
		dir := b.userDir(user)
		if filepath.Dir(dir) != b.dir || strings.Contains(filepath.Base(dir), "..") || strings.HasPrefix(filepath.Base(dir), ".") {
			t.Errorf("user %q got dir %q", user, dir)
		}
	}
	if _, ok := b.path("u-1", "../x.log.gz"); ok {
		t.Error("a path with .. was accepted")
	}
}

// The admin endpoint asks every socket of the user; a user with no socket
// here is "not_connected".
func TestRequestLogsReachesTheUsersSocket(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	s.bundles = newBundleManager(filepath.Join(t.TempDir(), "bundles"), time.Hour, s.Logger)

	rec := httptest.NewRecorder()
	s.handleAdminUser(rec, httptest.NewRequest("POST", "/admin/api/user/u-1/request-logs", nil))
	if !strings.Contains(rec.Body.String(), `"not_connected"`) {
		t.Fatalf("no socket: %s", rec.Body.String())
	}

	probe := newWSProbe(t)
	peer := NewPeer("u-1", "member", probe.server, s.Logger)
	session := NewVoiceSession("voice:c:ch", "c", "ch", s.Logger)
	s.sessions.sessions[session.ID()] = session
	if err := session.AddPeer(peer); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	s.handleAdminUser(rec, httptest.NewRequest("POST", "/admin/api/user/u-1/request-logs", nil))
	var out struct {
		Status    string `json:"status"`
		RequestID string `json:"request_id"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&out)
	if out.Status != "sent" || len(out.RequestID) != 32 {
		t.Fatalf("request = %+v", out)
	}
	deadline := time.Now().Add(2 * time.Second)
	for probe.count("request_logs") == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	msgs := probe.messages("request_logs")
	if len(msgs) != 1 {
		t.Fatalf("request_logs messages = %d", len(msgs))
	}
	var d requestLogsData
	_ = json.Unmarshal(msgs[0].Data, &d)
	if d.RequestID != out.RequestID || d.MaxBytes != bundleMaxBytes || d.SinceHours != 24 {
		t.Fatalf("request_logs data = %+v", d)
	}

	// The client's answer on its socket is stored, and the endpoints list,
	// show and serve it.
	s.handleLogsBundle(peer, "u-1", chunkMsg(out.RequestID, 0, true, gz(t, "hello\nworld\n")))
	rec = httptest.NewRecorder()
	s.handleAdminUser(rec, httptest.NewRequest("GET", "/admin/api/user/u-1/bundles", nil))
	var list struct {
		Bundles []BundleFile `json:"bundles"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&list)
	if len(list.Bundles) != 1 {
		t.Fatalf("bundles = %+v", list)
	}
	rec = httptest.NewRecorder()
	s.handleAdminUser(rec, httptest.NewRequest("GET", "/admin/api/user/u-1/bundles/"+list.Bundles[0].Name+"?q=wor", nil))
	if !strings.Contains(rec.Body.String(), `"world"`) {
		t.Fatalf("view = %s", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.handleAdminUser(rec, httptest.NewRequest("GET", "/admin/api/user/u-1/bundles/"+list.Bundles[0].Name+"/download", nil))
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/gzip" {
		t.Fatalf("download = %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
}
