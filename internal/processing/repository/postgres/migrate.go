package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func Migrate(ctx context.Context, db executor) error {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := fmt.Sprintf("migrations/%s", entry.Name())
		query, err := migrationFiles.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := db.Exec(ctx, string(query)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
	}

	return nil
}
