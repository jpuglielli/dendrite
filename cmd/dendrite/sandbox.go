package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jpuglielli/dendrite/internal/contrib"
	"github.com/jpuglielli/dendrite/internal/sandbox"
	"github.com/spf13/cobra"
)

var sandboxProgramID int

func init() {
	sandboxCreateCmd.Flags().IntVar(&sandboxProgramID, "program-id", 0, "WEBOM program ID to branch")
	_ = sandboxCreateCmd.MarkFlagRequired("program-id")

	sandboxCmd.AddCommand(sandboxCreateCmd)
	sandboxCmd.AddCommand(sandboxListCmd)
	sandboxCmd.AddCommand(sandboxDestroyCmd)
	rootCmd.AddCommand(sandboxCmd)
}

var sandboxCmd = &cobra.Command{
	Use:   "sandbox",
	Short: "Manage isolated sandbox databases",
}

var sandboxCreateCmd = &cobra.Command{
	Use:   "create --program-id N",
	Short: "Create a new sandbox with WEBOM data",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		mgr, err := sandbox.NewManager(buildAdminDSN())
		if err != nil {
			return fmt.Errorf("connecting to admin db: %w", err)
		}
		defer mgr.Close()

		srcDSN, err := connectSource(ctx)
		if err != nil {
			return err
		}

		specs := contrib.WEBOMSpecs(sandboxProgramID)

		fmt.Printf("Creating sandbox for program_id=%d (%d copy specs)\n", sandboxProgramID, len(specs))
		t := time.Now()

		sb, err := mgr.Create(ctx, srcDSN, specs)
		if err != nil {
			return fmt.Errorf("creating sandbox: %w", err)
		}

		fmt.Printf("Sandbox created in %s\n", time.Since(t))
		fmt.Printf("  ID:  %s\n", sb.ID)
		fmt.Printf("  DSN: %s\n", sb.DSN)
		return nil
	},
}

var sandboxListCmd = &cobra.Command{
	Use:   "list",
	Short: "List active sandboxes",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
		defer cancel()

		mgr, err := sandbox.NewManager(buildAdminDSN())
		if err != nil {
			return fmt.Errorf("connecting to admin db: %w", err)
		}
		defer mgr.Close()

		sandboxes, err := mgr.List(ctx)
		if err != nil {
			return err
		}

		if len(sandboxes) == 0 {
			fmt.Println("No active sandboxes.")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "ID\tDATABASE\tROLE")
		for _, sb := range sandboxes {
			_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", sb.ID, sb.Database, sb.Role)
		}
		return w.Flush()
	},
}

var sandboxDestroyCmd = &cobra.Command{
	Use:   "destroy ID",
	Short: "Destroy a sandbox by ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id := args[0]

		// Basic validation: expect a hex string.
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

		fmt.Printf("Destroying sandbox %s ...\n", id)
		if err := mgr.Destroy(ctx, id); err != nil {
			return fmt.Errorf("destroying sandbox: %w", err)
		}

		fmt.Println("Done.")
		return nil
	},
}
