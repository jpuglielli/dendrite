package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jpuglielli/dendrite/internal/postgres"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(tempCmd)
}

var tempCmd = &cobra.Command{
	Use:   `temp "QUERY:TABLE" ...`,
	Short: "Branch (schema + data) with per-step timing",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		total := time.Now()

		specs, err := parseSpecs(args)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		// Connect to source
		t := time.Now()
		srcDSN, err := connectSource(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("[connect]      %s\n", time.Since(t))

		dstDSN := buildDstDSN()

		// Copy schema
		t = time.Now()
		if err := postgres.CopySchema(ctx, srcDSN, dstDSN); err != nil {
			return fmt.Errorf("schema copy failed: %w", err)
		}
		fmt.Printf("[copy-schema]  %s\n", time.Since(t))

		// Copy data (all tables in a single REPEATABLE READ snapshot)
		tableStart := time.Now()
		if err := postgres.CopyDataWithCallback(ctx, srcDSN, dstDSN, specs, func(spec postgres.CopySpec) {
			fmt.Printf("[copy-data]    %-30s %s\n", spec.Table, time.Since(tableStart))
			tableStart = time.Now()
		}); err != nil {
			return fmt.Errorf("data copy failed: %w", err)
		}

		fmt.Printf("[total]        %s\n", time.Since(total))
		return nil
	},
}
