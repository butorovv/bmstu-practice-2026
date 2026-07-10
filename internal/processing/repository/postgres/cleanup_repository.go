package postgres

import (
	"context"
	"time"
)

type CleanupRepository struct {
	db executor
}

func NewCleanupRepository(db executor) *CleanupRepository {
	return &CleanupRepository{db: db}
}

func (r *CleanupRepository) DeleteOlderThan(ctx context.Context, cutoff time.Time) error {
	if _, err := r.db.Exec(ctx, `DELETE FROM telemetry WHERE created_at < $1`, cutoff); err != nil {
		return err
	}
	if _, err := r.db.Exec(ctx, `DELETE FROM alerts WHERE created_at < $1`, cutoff); err != nil {
		return err
	}

	return nil
}
