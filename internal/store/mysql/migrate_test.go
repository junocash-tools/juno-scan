//go:build mysql

package mysql

import (
	"errors"
	"strings"
	"testing"

	driver "github.com/go-sql-driver/mysql"
)

func TestCrashResumeDDLErrorClassification(t *testing.T) {
	for _, code := range []uint16{1050, 1060, 1061} {
		if !isCrashResumeDDLError(&driver.MySQLError{Number: code, Message: "already applied"}) {
			t.Fatalf("DDL code %d must be resumable", code)
		}
	}
	if isCrashResumeDDLError(&driver.MySQLError{Number: 1062, Message: "duplicate data"}) {
		t.Fatal("duplicate data must remain fail-closed")
	}
	if isCrashResumeDDLError(errors.New("other")) {
		t.Fatal("non-MySQL errors must remain fatal")
	}
}

func TestWalletUpgradeMigrationsAreStatementwiseResumableAndSeedProgress(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	byVersion := make(map[int]migration)
	for _, migration := range migrations {
		byVersion[migration.version] = migration
	}
	for _, version := range []int{12, 14, 15, 16} {
		migration, ok := byVersion[version]
		if !ok || len(splitSQLStatements(migration.sql)) == 0 {
			t.Fatalf("migration %d missing or unparsable", version)
		}
	}
	if sql := strings.ToLower(byVersion[12].sql); !strings.Contains(sql, "insert ignore into wallet_backfill_progress") || !strings.Contains(sql, "select wallet_id,birthday_height") {
		t.Fatal("migration 0012 does not seed progress for existing wallets")
	}
	if sql := strings.ToLower(byVersion[14].sql); !strings.Contains(sql, "generated always") || !strings.Contains(sql, "unique index") {
		t.Fatal("migration 0014 does not recreate both generated identity and uniqueness statements")
	}
	if sql := strings.ToLower(byVersion[15].sql); !strings.Contains(sql, "generation") || !strings.Contains(sql, "insert ignore") {
		t.Fatal("migration 0015 does not backfill generation and missing progress")
	}
	if sql := strings.ToLower(byVersion[16].sql); !strings.Contains(sql, "expired_height") || !strings.Contains(sql, "create index") || !strings.Contains(sql, "outgoingoutputexpired") || !strings.Contains(sql, "9223372036854775807") {
		t.Fatal("migration 0016 does not add, recover, and conservatively seed expiry observation height")
	}
}
