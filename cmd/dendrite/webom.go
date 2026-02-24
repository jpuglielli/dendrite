package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jpuglielli/dendrite/internal/contrib"
	"github.com/jpuglielli/dendrite/internal/postgres"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(webomCmd)
}

var webomCmd = &cobra.Command{
	Use:   "webom PROGRAM_ID",
	Short: "Branch schema + WEBOM data for a program",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		programID, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("invalid program ID %q: %w", args[0], err)
		}

		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		srcDSN, err := connectSource(ctx)
		if err != nil {
			return err
		}
		dstDSN := buildDstDSN()

		specs := contrib.WEBOMSpecs(programID)

		fmt.Printf("Branching WEBOM for program_id=%d\n", programID)
		fmt.Printf("  dst: %s\n", dstDSN)
		fmt.Printf("  %d copy spec(s)\n", len(specs))

		total := time.Now()

		t := time.Now()
		if err := postgres.CopySchema(ctx, srcDSN, dstDSN); err != nil {
			return fmt.Errorf("schema copy failed: %w", err)
		}
		fmt.Printf("[copy-schema]  %s\n", time.Since(t))

		for _, spec := range specs {
			t = time.Now()
			if err := postgres.CopyData(ctx, srcDSN, dstDSN, []postgres.CopySpec{spec}); err != nil {
				return fmt.Errorf("copy failed for %s: %w", spec.Table, err)
			}
			fmt.Printf("[copy-data]    %-35s %s\n", spec.Table, time.Since(t))
		}

		fmt.Printf("[total]        %s\n", time.Since(total))
		return nil
	},
}
