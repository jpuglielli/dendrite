package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jpuglielli/dendrite/internal/postgres"
	"github.com/spf13/cobra"
)

var (
	dstFlag     string
	timeoutFlag time.Duration
)

var rootCmd = &cobra.Command{
	Use:   "dendrite",
	Short: "Dendrite — selective Postgres branching tool",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&dstFlag, "dst", "", "destination DSN (default: compose DB at localhost:15432)")
	rootCmd.PersistentFlags().DurationVar(&timeoutFlag, "timeout", 5*time.Minute, "operation timeout")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// buildAdminDSN returns a DSN targeting the "postgres" maintenance database
// so the sandbox manager can CREATE/DROP databases without being connected to them.
func buildAdminDSN() string {
	user := envOrDefault("DST_USER", "devuser")
	pass := envOrDefault("DST_PASSWORD", "devpass")
	host := envOrDefault("DST_HOST", "localhost")
	port := envOrDefault("DST_PORT", "15432")

	return fmt.Sprintf("postgres://%s:%s@%s:%s/postgres", user, pass, host, port)
}

// buildDstDSN returns the destination DSN from --dst flag or DST_* env vars,
// falling back to the local compose DB defaults.
func buildDstDSN() string {
	if dstFlag != "" {
		return dstFlag
	}

	user := envOrDefault("DST_USER", "devuser")
	pass := envOrDefault("DST_PASSWORD", "devpass")
	host := envOrDefault("DST_HOST", "localhost")
	port := envOrDefault("DST_PORT", "15432")
	name := envOrDefault("DST_NAME", "dendrite_dev")

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, name)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// connectSource loads .env and returns the source DSN.
func connectSource(ctx context.Context) (string, error) {
	pool, err := postgres.Connect(ctx)
	if err != nil {
		return "", fmt.Errorf("connecting to source: %w", err)
	}
	defer pool.Close()
	return postgres.DSN(pool), nil
}

// parseSpecs splits "QUERY:TABLE" args on the last ":" to handle :: typecasts.
func parseSpecs(args []string) ([]postgres.CopySpec, error) {
	specs := make([]postgres.CopySpec, 0, len(args))
	for _, arg := range args {
		idx := strings.LastIndex(arg, ":")
		if idx <= 0 {
			return nil, fmt.Errorf("invalid spec %q: expected QUERY:TABLE", arg)
		}
		query := arg[:idx]
		table := arg[idx+1:]
		if query == "" || table == "" {
			return nil, fmt.Errorf("invalid spec %q: query and table must be non-empty", arg)
		}
		specs = append(specs, postgres.CopySpec{Query: query, Table: table})
	}
	return specs, nil
}
