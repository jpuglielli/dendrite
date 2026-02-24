package main

import (
	"context"
	"fmt"

	"github.com/jpuglielli/dendrite/internal/postgres"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(schemaCmd)
}

var schemaCmd = &cobra.Command{
	Use:   "copy-schema",
	Short: "Copy schema only from source to destination (no data)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		srcDSN, err := connectSource(ctx)
		if err != nil {
			return err
		}
		dstDSN := buildDstDSN()

		fmt.Printf("Copying schema to %s\n", dstDSN)

		if err := postgres.CopySchema(ctx, srcDSN, dstDSN); err != nil {
			return fmt.Errorf("schema copy failed: %w", err)
		}

		fmt.Println("Schema copy complete.")
		return nil
	},
}
