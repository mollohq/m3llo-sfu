package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
)

// Client log bundles (the media admin brief, section 2.4; 01-SFU.md
// section 8.8). An admin asks a connected user's client for its app log:
// the SFU sends `request_logs` on the user's signaling socket, the client
// gzips its last 24 h of app log (5 MB at most) and sends it back on the
// same socket in `logs_bundle` chunks. The socket is the client's
// JWT-authenticated connection, so the upload needs no new route and no new
// credential. The SFU keeps each bundle as a file on the store's volume
// (not in SQLite), one directory per user, for the store's retention.

const (
	bundleMaxBytes   = 5 << 20
	bundleChunkBytes = 192 << 10
	// A request the client does not answer in this time is expired.
	bundleRequestTTL = 10 * time.Minute
	bundleSinceHours = 24
	// The viewer of a bundle returns at most this many lines.
	bundleViewMaxLines = 2000
)

var (
	safeUserDir    = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,127}$`)
	safeBundleName = regexp.MustCompile(`^[0-9]{13}-[0-9a-f]{32}\.log\.gz$`)
	safeRequestID  = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

type bundleRequest struct {
	ID        string    `json:"request_id"`
	User      string    `json:"-"`
	Requested time.Time `json:"-"`
	// Status: sent, receiving, stored, failed, expired.
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Bytes  int64  `json:"bytes"`
	File   string `json:"file,omitempty"`
	Peers  int    `json:"peers"`

	RequestedMs int64 `json:"requested_ms"`
	buf         bytes.Buffer
	nextSeq     int
}

// BundleFile is one stored bundle.
type BundleFile struct {
	Name      string `json:"name"`
	RequestID string `json:"request_id"`
	Ms        int64  `json:"ms"`
	Bytes     int64  `json:"bytes"`
}

type bundleManager struct {
	dir       string
	retention time.Duration
	logger    *logging.Logger

	mu      sync.Mutex
	pending map[string]*bundleRequest
}

func newBundleManager(dir string, retention time.Duration, logger *logging.Logger) *bundleManager {
	return &bundleManager{dir: dir, retention: retention, logger: logger, pending: map[string]*bundleRequest{}}
}

// userDir is the user's directory under the bundle root. A user id that is
// not a plain name is hex-encoded, so no id can leave the root.
func (b *bundleManager) userDir(user string) string {
	name := user
	if !safeUserDir.MatchString(user) {
		name = "x" + hex.EncodeToString([]byte(user))
	}
	return filepath.Join(b.dir, name)
}

func newRequestID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// start records a request for user, sent to peers sockets.
func (b *bundleManager) start(user string, peers int, now time.Time) *bundleRequest {
	r := &bundleRequest{ID: newRequestID(), User: user, Requested: now, RequestedMs: now.UnixMilli(),
		Status: "sent", Peers: peers}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expireLocked(now)
	b.pending[r.ID] = r
	return r
}

func (b *bundleManager) expireLocked(now time.Time) {
	for id, r := range b.pending {
		age := now.Sub(r.Requested)
		switch {
		case (r.Status == "sent" || r.Status == "receiving") && age > bundleRequestTTL:
			r.Status, r.Error = "expired", "no answer from the client"
			r.buf = bytes.Buffer{}
		case age > 24*time.Hour:
			delete(b.pending, id)
		}
	}
}

// logsBundleData is one `logs_bundle` message from a client.
type logsBundleData struct {
	RequestID string `json:"request_id"`
	Seq       int    `json:"seq"`
	Last      bool   `json:"last"`
	Data      string `json:"data"`
	Error     string `json:"error"`
}

var errBundleRefused = errors.New("bundle chunk refused")

// chunk takes one chunk from user's socket. The bundle is written at the
// last chunk. A chunk for another user's request, out of order, or over the
// cap fails the request.
func (b *bundleManager) chunk(user string, raw json.RawMessage, now time.Time) error {
	var d logsBundleData
	if err := json.Unmarshal(raw, &d); err != nil || !safeRequestID.MatchString(d.RequestID) {
		return errBundleRefused
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.pending[d.RequestID]
	if !ok || r.User != user || (r.Status != "sent" && r.Status != "receiving") {
		return errBundleRefused
	}
	fail := func(msg string) error {
		r.Status, r.Error = "failed", msg
		r.buf = bytes.Buffer{}
		return fmt.Errorf("%w: %s", errBundleRefused, msg)
	}
	if d.Error != "" {
		return fail("client: " + truncate(d.Error, 200))
	}
	if d.Seq != r.nextSeq {
		return fail(fmt.Sprintf("chunk %d, want %d", d.Seq, r.nextSeq))
	}
	part, err := base64.StdEncoding.DecodeString(d.Data)
	if err != nil {
		return fail("chunk is not base64")
	}
	if r.buf.Len()+len(part) > bundleMaxBytes {
		return fail(fmt.Sprintf("over %d bytes", bundleMaxBytes))
	}
	r.buf.Write(part)
	r.nextSeq++
	r.Status = "receiving"
	r.Bytes = int64(r.buf.Len())
	if !d.Last {
		return nil
	}
	data := r.buf.Bytes()
	if _, err := gzip.NewReader(bytes.NewReader(data)); err != nil {
		return fail("bundle is not gzip")
	}
	dir := b.userDir(user)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fail("cannot store: " + err.Error())
	}
	name := fmt.Sprintf("%013d-%s.log.gz", now.UnixMilli(), r.ID)
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		return fail("cannot store: " + err.Error())
	}
	r.Status, r.File = "stored", name
	r.buf = bytes.Buffer{}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// list returns user's bundles, newest first, and the requests of the last
// day.
func (b *bundleManager) list(user string, now time.Time) ([]BundleFile, []bundleRequest) {
	files := []BundleFile{}
	entries, _ := os.ReadDir(b.userDir(user))
	for _, e := range entries {
		name := e.Name()
		if !safeBundleName.MatchString(name) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		ms, _ := strconv.ParseInt(name[:13], 10, 64)
		files = append(files, BundleFile{Name: name, RequestID: name[14:46], Ms: ms, Bytes: info.Size()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Ms > files[j].Ms })
	b.mu.Lock()
	defer b.mu.Unlock()
	b.expireLocked(now)
	reqs := []bundleRequest{}
	for _, r := range b.pending {
		if r.User == user {
			reqs = append(reqs, bundleRequest{ID: r.ID, Status: r.Status, Error: r.Error, Bytes: r.Bytes,
				File: r.File, Peers: r.Peers, RequestedMs: r.RequestedMs})
		}
	}
	sort.Slice(reqs, func(i, j int) bool { return reqs[i].RequestedMs > reqs[j].RequestedMs })
	return files, reqs
}

// path is the file of user's bundle name; ok false for a name that is not
// a bundle's.
func (b *bundleManager) path(user, name string) (string, bool) {
	if !safeBundleName.MatchString(name) {
		return "", false
	}
	p := filepath.Join(b.userDir(user), name)
	if _, err := os.Stat(p); err != nil {
		return "", false
	}
	return p, true
}

// view reads a bundle and returns its last lines, or with q the lines that
// contain q (case-insensitive), at most limit of them.
func (b *bundleManager) view(path, q string, limit int) (lines []string, matched, total int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, 0, 0, err
	}
	sc := bufio.NewScanner(io.LimitReader(zr, 64<<20))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	needle := strings.ToLower(q)
	ring := make([]string, 0, limit)
	for sc.Scan() {
		total++
		line := sc.Text()
		if needle != "" && !strings.Contains(strings.ToLower(line), needle) {
			continue
		}
		matched++
		if len(ring) == limit {
			ring = ring[1:]
		}
		ring = append(ring, line)
	}
	return ring, matched, total, sc.Err()
}

// sweep deletes the bundles older than the retention.
func (b *bundleManager) sweep(now time.Time) {
	cutoff := now.Add(-b.retention).UnixMilli()
	users, _ := os.ReadDir(b.dir)
	for _, u := range users {
		if !u.IsDir() {
			continue
		}
		dir := filepath.Join(b.dir, u.Name())
		files, _ := os.ReadDir(dir)
		for _, f := range files {
			if !safeBundleName.MatchString(f.Name()) {
				continue
			}
			if ms, _ := strconv.ParseInt(f.Name()[:13], 10, 64); ms < cutoff {
				_ = os.Remove(filepath.Join(dir, f.Name()))
			}
		}
	}
}
