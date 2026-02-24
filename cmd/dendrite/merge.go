package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jpuglielli/dendrite/internal/merge"
	"github.com/jpuglielli/dendrite/internal/sandbox"
	"github.com/jpuglielli/dendrite/internal/tracker"
	"github.com/spf13/cobra"
)

var diffFormat string

var sandboxDiffCmd = &cobra.Command{
	Use:   "diff ID",
	Short: "Show changes in a sandbox compared to its source",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id := args[0]
		if _, err := hex.DecodeString(id); err != nil {
			return fmt.Errorf("invalid sandbox ID %q: expected hex string", id)
		}

		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		mgr, err := sandbox.NewManager(buildAdminDSN())
		if err != nil {
			return fmt.Errorf("connecting to admin db: %w", err)
		}
		defer mgr.Close()

		sbPool, err := mgr.ConnectSandbox(ctx, id)
		if err != nil {
			return err
		}
		defer sbPool.Close()

		srcDSN, tables, err := tracker.ReadMeta(ctx, sbPool)
		if err != nil {
			return err
		}

		srcPool, err := pgxpool.New(ctx, srcDSN)
		if err != nil {
			return fmt.Errorf("connecting to source: %w", err)
		}
		defer srcPool.Close()

		diff, err := merge.Diff(ctx, srcPool, sbPool, tables)
		if err != nil {
			return fmt.Errorf("computing diff: %w", err)
		}

		switch diffFormat {
		case "sql":
			stmts := merge.GenerateSQL(diff)
			if len(stmts) == 0 {
				fmt.Println("No changes.")
				return nil
			}
			for _, s := range stmts {
				fmt.Println(s)
			}
		default: // "summary"
			printDiffSummary(diff)
		}

		return nil
	},
}

var (
	mergeDryRun bool
	mergeForce  bool
)

var sandboxMergeCmd = &cobra.Command{
	Use:   "merge ID",
	Short: "Merge sandbox changes back into the source database",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id := args[0]
		if _, err := hex.DecodeString(id); err != nil {
			return fmt.Errorf("invalid sandbox ID %q: expected hex string", id)
		}

		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		mgr, err := sandbox.NewManager(buildAdminDSN())
		if err != nil {
			return fmt.Errorf("connecting to admin db: %w", err)
		}
		defer mgr.Close()

		sbPool, err := mgr.ConnectSandbox(ctx, id)
		if err != nil {
			return err
		}
		defer sbPool.Close()

		srcDSN, tables, err := tracker.ReadMeta(ctx, sbPool)
		if err != nil {
			return err
		}

		srcPool, err := pgxpool.New(ctx, srcDSN)
		if err != nil {
			return fmt.Errorf("connecting to source: %w", err)
		}
		defer srcPool.Close()

		diff, err := merge.Diff(ctx, srcPool, sbPool, tables)
		if err != nil {
			return fmt.Errorf("computing diff: %w", err)
		}

		if len(diff.Conflicts) > 0 {
			printConflicts(diff.Conflicts)
			if !mergeForce {
				return fmt.Errorf("aborting due to %d conflict(s) — use --force to skip conflicted rows", len(diff.Conflicts))
			}
			fmt.Printf("--force: skipping %d conflicted row(s)\n\n", len(diff.Conflicts))
		}

		if mergeDryRun {
			stmts := merge.GenerateSQL(diff)
			if len(stmts) == 0 {
				fmt.Println("No changes to apply.")
				return nil
			}
			fmt.Println("-- DRY RUN: the following SQL would be executed:")
			for _, s := range stmts {
				fmt.Println(s)
			}
			return nil
		}

		opts := merge.MergeOpts{
			Force:      mergeForce,
			MaxRetries: 3,
		}
		if err := merge.Merge(ctx, srcPool, diff, tables, opts); err != nil {
			return fmt.Errorf("merge failed: %w", err)
		}

		printMergeSummary(diff)
		return nil
	},
}

func printDiffSummary(diff *merge.DiffResult) {
	total := 0
	for _, td := range diff.Tables {
		n := len(td.Inserts) + len(td.Updates) + len(td.Deletes)
		if n == 0 {
			continue
		}
		total += n
		fmt.Printf("%s: %d insert(s), %d update(s), %d delete(s)\n",
			td.Table, len(td.Inserts), len(td.Updates), len(td.Deletes))
	}
	if total == 0 && len(diff.Conflicts) == 0 {
		fmt.Println("No changes.")
		return
	}
	if len(diff.Conflicts) > 0 {
		fmt.Printf("\n%d conflict(s):\n", len(diff.Conflicts))
		printConflicts(diff.Conflicts)
	}
}

func printConflicts(conflicts []merge.Conflict) {
	for _, c := range conflicts {
		pkParts := make([]string, 0, len(c.PKVals))
		for k, v := range c.PKVals {
			pkParts = append(pkParts, fmt.Sprintf("%s=%v", k, v))
		}
		fmt.Printf("  CONFLICT %s %s [%s]: %s\n",
			c.Op, c.Table, strings.Join(pkParts, ", "), c.Reason)
	}
}

func printMergeSummary(diff *merge.DiffResult) {
	var inserts, updates, deletes int
	for _, td := range diff.Tables {
		inserts += len(td.Inserts)
		updates += len(td.Updates)
		deletes += len(td.Deletes)
	}
	fmt.Printf("Merge complete: %d insert(s), %d update(s), %d delete(s)\n",
		inserts, updates, deletes)
}
