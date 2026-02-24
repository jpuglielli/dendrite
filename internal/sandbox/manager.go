package sandbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpuglielli/dendrite/internal/postgres"
	"github.com/jpuglielli/dendrite/internal/tracker"
)

// Manager handles the lifecycle of sandbox databases within a shared Postgres instance.
type Manager struct {
	pool *pgxpool.Pool
	host string
	port string
}

// Sandbox represents an isolated database environment.
type Sandbox struct {
	ID       string // 16-char hex identifier
	Database string // "sandbox_<id>"
	Role     string // "sandbox_<id>"
	Password string // 32-char hex (only populated on Create)
	DSN      string // full connection string (only populated on Create)
}

// NewManager connects to Postgres as admin via the maintenance database.
// The admin DSN should target the "postgres" database so the manager can
// CREATE/DROP other databases without being connected to them.
func NewManager(adminDSN string) (*Manager, error) {
	cfg, err := pgxpool.ParseConfig(adminDSN)
	if err != nil {
		return nil, fmt.Errorf("parsing admin DSN: %w", err)
	}

	// Use simple protocol so CREATE DATABASE / DROP DATABASE work
	// (extended protocol wraps statements in implicit transactions).
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	host := cfg.ConnConfig.Host
	port := fmt.Sprintf("%d", cfg.ConnConfig.Port)

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating admin pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging admin db: %w", err)
	}

	return &Manager{pool: pool, host: host, port: port}, nil
}

// Create provisions a new sandbox database, copies schema+data from srcDSN
// using the provided copy specs, and returns the sandbox details including DSN.
func (m *Manager) Create(ctx context.Context, srcDSN string, specs []postgres.CopySpec) (*Sandbox, error) {
	id := randomID()
	password := randomPassword()
	name := "sandbox_" + id

	sb := &Sandbox{
		ID:       id,
		Database: name,
		Role:     name,
		Password: password,
	}

	// Create role and database.
	stmts := []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD %s`, pgIdent(name), pgLiteral(password)),
		fmt.Sprintf(`CREATE DATABASE %s OWNER %s`, pgIdent(name), pgIdent(name)),
		fmt.Sprintf(`REVOKE CONNECT ON DATABASE %s FROM PUBLIC`, pgIdent(name)),
	}
	for _, stmt := range stmts {
		if _, err := m.pool.Exec(ctx, stmt); err != nil {
			// Best-effort cleanup on partial creation.
			_ = m.destroyByName(ctx, name)
			return nil, fmt.Errorf("exec %q: %w", stmt, err)
		}
	}

	// Build DSN for the new sandbox.
	cfg := m.pool.Config().ConnConfig
	user := cfg.User
	pass := cfg.Password
	sb.DSN = fmt.Sprintf("postgres://%s:%s@%s/sandbox_%s",
		user, pass, net.JoinHostPort(m.host, m.port), id)

	// Branch schema + data from source into sandbox.
	if err := postgres.Branch(ctx, srcDSN, sb.DSN, specs); err != nil {
		_ = m.destroyByName(ctx, name)
		return nil, fmt.Errorf("branching into sandbox: %w", err)
	}

	// Install change-tracking triggers and metadata.
	sbPool, err := pgxpool.New(ctx, sb.DSN)
	if err != nil {
		_ = m.destroyByName(ctx, name)
		return nil, fmt.Errorf("connecting to sandbox for tracking: %w", err)
	}
	defer sbPool.Close()

	tableNames := postgres.TableNames(specs)
	if err := tracker.WriteMeta(ctx, sbPool, srcDSN, tableNames); err != nil {
		_ = m.destroyByName(ctx, name)
		return nil, fmt.Errorf("writing sandbox metadata: %w", err)
	}
	if err := tracker.Install(ctx, sbPool, tableNames); err != nil {
		_ = m.destroyByName(ctx, name)
		return nil, fmt.Errorf("installing change tracking: %w", err)
	}

	return sb, nil
}

// List returns all active sandboxes (databases with the sandbox_ prefix).
func (m *Manager) List(ctx context.Context) ([]Sandbox, error) {
	rows, err := m.pool.Query(ctx, `SELECT datname FROM pg_database WHERE datname LIKE 'sandbox_%' ORDER BY datname`)
	if err != nil {
		return nil, fmt.Errorf("listing sandboxes: %w", err)
	}
	defer rows.Close()

	var sandboxes []Sandbox
	for rows.Next() {
		var datname string
		if err := rows.Scan(&datname); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		id := strings.TrimPrefix(datname, "sandbox_")
		sandboxes = append(sandboxes, Sandbox{
			ID:       id,
			Database: datname,
			Role:     datname,
		})
	}
	return sandboxes, rows.Err()
}

// Destroy removes a sandbox by its ID, terminating connections and dropping
// the database and role.
func (m *Manager) Destroy(ctx context.Context, id string) error {
	return m.destroyByName(ctx, "sandbox_"+id)
}

// ConnectSandbox returns a pgxpool connected to the sandbox database
// identified by the given hex ID. Uses admin credentials.
func (m *Manager) ConnectSandbox(ctx context.Context, id string) (*pgxpool.Pool, error) {
	cfg := m.pool.Config().ConnConfig
	dsn := fmt.Sprintf("postgres://%s:%s@%s/sandbox_%s",
		cfg.User, cfg.Password, net.JoinHostPort(m.host, m.port), id)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connecting to sandbox_%s: %w", id, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging sandbox_%s: %w", id, err)
	}
	return pool, nil
}

// Close releases the admin connection pool.
func (m *Manager) Close() {
	m.pool.Close()
}

func (m *Manager) destroyByName(ctx context.Context, name string) error {
	stmts := []string{
		// Terminate any active connections to the sandbox database.
		fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()`, pgLiteral(name)),
		fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, pgIdent(name)),
		fmt.Sprintf(`DROP ROLE IF EXISTS %s`, pgIdent(name)),
	}
	var firstErr error
	for _, stmt := range stmts {
		if _, err := m.pool.Exec(ctx, stmt); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// randomID returns a 16-character hex string (8 random bytes).
func randomID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// randomPassword returns a 32-character hex string (16 random bytes).
func randomPassword() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// pgIdent double-quotes a SQL identifier to prevent injection.
func pgIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// pgLiteral single-quotes a SQL literal to prevent injection.
func pgLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}
