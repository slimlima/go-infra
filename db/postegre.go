package db

import (
	"context"
	"fmt"
	"log"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

type PostgreDataBaseManager struct {
	Conn *pg.DB
}

type txKey struct{}

func NewPostgreDataBaseConnection(options *pg.Options) (con *pg.DB) {
	return pg.Connect(options)
}

func NewPostgreDataBaseManager(conn *pg.DB) *PostgreDataBaseManager {
	return &PostgreDataBaseManager{
		Conn: conn,
	}
}

func (db *PostgreDataBaseManager) model(ctx context.Context, model ...interface{}) *orm.Query {
	tx := extractTx(ctx)
	if tx != nil {
		return tx.ModelContext(ctx, model...)
	}
	return db.Conn.ModelContext(ctx, model...)
}

func (db *PostgreDataBaseManager) WithinTransaction(ctx context.Context, tFunc func(ctx context.Context) error) error {
	// begin transaction
	tx, err := db.Conn.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		// finalize transaction on panic, etc.
		if errTx := tx.Close(); errTx != nil {
			log.Printf("close transaction: %v", errTx)
		}
	}()

	// run callback
	err = tFunc(injectTx(ctx, tx))
	if err != nil {
		// if error, rollback
		if errRollback := tx.Rollback(); errRollback != nil {
			log.Printf("rollback transaction: %v", errRollback)
		}
		return err
	}
	// if no error, commit
	if errCommit := tx.Commit(); errCommit != nil {
		log.Printf("commit transaction: %v", errCommit)
	}
	return nil
}

// injectTx injects transaction to context
func injectTx(ctx context.Context, tx *pg.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// extractTx extracts transaction from context
func extractTx(ctx context.Context) *pg.Tx {
	if tx, ok := ctx.Value(txKey{}).(*pg.Tx); ok {
		return tx
	}
	return nil
}
