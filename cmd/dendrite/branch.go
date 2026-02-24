package main

import (
	"context"
	"fmt"

	"github.com/jpuglielli/dendrite/internal/postgres"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(branchCmd)
}

var branchCmd = &cobra.Command{
	Use:   `branch "QUERY:TABLE" ...`,
	Short: "Copy schema and selective data from source to destination",
	Long: `Branch copies the full schema from the source database and then
selectively copies data for each provided copy spec.

Each positional argument is a "QUERY:TABLE" pair where QUERY is a SELECT
statement to run against the source and TABLE is the destination table.
Split is on the last ":" so PostgreSQL :: typecasts work fine.`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		specs, err := parseSpecs(args)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		srcDSN, err := connectSource(ctx)
		if err != nil {
			return err
		}
		dstDSN := buildDstDSN()

		fmt.Printf("Branching from source to %s\n", dstDSN)
		fmt.Printf("  %d copy spec(s)\n", len(specs))

		if err := postgres.Branch(ctx, srcDSN, dstDSN, specs); err != nil {
			return fmt.Errorf("branch failed: %w", err)
		}

		fmt.Println("Branch complete.")
		return nil
	},
}
