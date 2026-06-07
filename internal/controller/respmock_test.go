/*
Copyright 2026 The Wellcake Authors.
*/

package controller

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cachev1beta1 "github.com/melancholictheory/wellcake/api/v1beta1"
)

// respMock is a minimal in-process RESP server for unit-testing the operator's
// go-redis wrappers. miniredis is unsuitable here: it implements neither
// REPLICAOF nor ACL SETUSER nor `INFO replication`, which is exactly the
// Valkey-specific surface our failover / ACL code drives. This mock instead
// gives full control — canned INFO/CLUSTER NODES payloads and a recording of
// every command received — so we can assert the wrappers issue the right
// arguments and parse realistic replies, with no external dependency.
type respMock struct {
	ln              net.Listener
	infoReplication string
	clusterNodes    string

	mu       sync.Mutex
	commands [][]string
}

func newRespMock(t *testing.T) *respMock {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	m := &respMock{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go m.serve()
	return m
}

// hostPort returns the mock's address split for dialReplClient.
func (m *respMock) hostPort(t *testing.T) (string, int32) {
	t.Helper()
	host, p, err := net.SplitHostPort(m.ln.Addr().String())
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	port, _ := strconv.Atoi(p)
	return host, int32(port)
}

// recorded returns the first recorded command whose head (joined, upper-cased)
// starts with the given prefix, e.g. "REPLICAOF" or "ACL SETUSER".
func (m *respMock) recorded(prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.commands {
		if strings.HasPrefix(strings.ToUpper(strings.Join(c, " ")), strings.ToUpper(prefix)) {
			return c
		}
	}
	return nil
}

func (m *respMock) serve() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			return
		}
		go m.handle(conn)
	}
}

func (m *respMock) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		m.mu.Lock()
		m.commands = append(m.commands, args)
		m.mu.Unlock()

		switch strings.ToUpper(args[0]) {
		case "HELLO":
			// Force a RESP2 fallback: go-redis tolerates a HELLO error and
			// continues on RESP2, which keeps this mock tiny.
			_, _ = io.WriteString(conn, "-ERR unknown command 'HELLO'\r\n")
		case "PING":
			_, _ = io.WriteString(conn, "+PONG\r\n")
		case "INFO":
			writeBulk(conn, m.infoReplication)
		case "CLUSTER":
			writeBulk(conn, m.clusterNodes)
		default:
			_, _ = io.WriteString(conn, "+OK\r\n")
		}
	}
}

// readCommand reads one RESP array-of-bulk-strings request (what go-redis sends).
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" || line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for range n {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if hdr == "" || hdr[0] != '$' {
			return nil, fmt.Errorf("expected bulk, got %q", hdr)
		}
		l, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, l+2) // payload + CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:l]))
	}
	return args, nil
}

func writeBulk(conn net.Conn, s string) {
	_, _ = io.WriteString(conn, fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

// --- tests ---

func TestReplClientInfoParsing(t *testing.T) {
	m := newRespMock(t)
	m.infoReplication = "# Replication\r\nrole:slave\r\nmaster_host:web-0.web-headless.ns.svc\r\nmaster_port:6379\r\nmaster_link_status:up\r\nslave_repl_offset:12345\r\n"
	host, port := m.hostPort(t)

	c := dialReplClient(context.Background(), host, port, "", false, nil, 2*time.Second)
	if c == nil {
		t.Fatal("dialReplClient returned nil (ping failed against mock)")
	}
	defer c.close()

	info, err := c.info(context.Background())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info["role"] != "slave" || info["master_link_status"] != "up" || info["slave_repl_offset"] != "12345" {
		t.Errorf("parsed INFO replication = %v", info)
	}
}

func TestReplClientReplicaOf(t *testing.T) {
	m := newRespMock(t)
	host, port := m.hostPort(t)
	c := dialReplClient(context.Background(), host, port, "", false, nil, 2*time.Second)
	if c == nil {
		t.Fatal("dial nil")
	}
	defer c.close()

	if err := c.replicaOf(context.Background(), "primary.host", 6380); err != nil {
		t.Fatalf("replicaOf: %v", err)
	}
	if got := m.recorded("REPLICAOF"); len(got) != 3 || got[1] != "primary.host" || got[2] != "6380" {
		t.Errorf("REPLICAOF args = %v, want [REPLICAOF primary.host 6380]", got)
	}

	if err := c.replicaOfNoOne(context.Background()); err != nil {
		t.Fatalf("replicaOfNoOne: %v", err)
	}
	if got := m.recorded("REPLICAOF NO ONE"); got == nil {
		t.Errorf("REPLICAOF NO ONE not issued; recorded: %v", m.commands)
	}
}

func TestApplyACLUserCommand(t *testing.T) {
	tests := []struct {
		name     string
		user     cachev1beta1.ValkeyACLUser
		password string
		wantTail []string // expected tokens after "ACL SETUSER <name> reset"
	}{
		{
			name:     "with rules and password",
			user:     cachev1beta1.ValkeyACLUser{Name: "alice", Rules: "on ~* +@read"},
			password: "pw",
			wantTail: []string{"on", "~*", "+@read", ">pw"},
		},
		{
			name:     "no password -> nopass",
			user:     cachev1beta1.ValkeyACLUser{Name: "bob", Rules: "on ~*"},
			password: "",
			wantTail: []string{"on", "~*", "nopass"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newRespMock(t)
			host, port := m.hostPort(t)
			c := dialReplClient(context.Background(), host, port, "", false, nil, 2*time.Second)
			if c == nil {
				t.Fatal("dial nil")
			}
			defer c.close()

			if err := applyACLUser(context.Background(), c, tc.user, tc.password); err != nil {
				t.Fatalf("applyACLUser: %v", err)
			}
			got := m.recorded("ACL SETUSER")
			if got == nil {
				t.Fatalf("ACL SETUSER not issued; recorded: %v", m.commands)
			}
			// Always: ACL SETUSER <name> reset ...
			want := append([]string{"ACL", "SETUSER", tc.user.Name, "reset"}, tc.wantTail...)
			if strings.Join(got, " ") != strings.Join(want, " ") {
				t.Errorf("ACL SETUSER args =\n  %v\nwant\n  %v", got, want)
			}
		})
	}
}

func TestReplClientClusterNodes(t *testing.T) {
	m := newRespMock(t)
	m.clusterNodes = "id1 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460\n"
	host, port := m.hostPort(t)
	c := dialReplClient(context.Background(), host, port, "", false, nil, 2*time.Second)
	if c == nil {
		t.Fatal("dial nil")
	}
	defer c.close()

	raw, err := c.clusterNodes(context.Background())
	if err != nil {
		t.Fatalf("clusterNodes: %v", err)
	}
	if !strings.Contains(raw, "myself,master") {
		t.Errorf("clusterNodes raw = %q", raw)
	}
}
