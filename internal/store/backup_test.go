package store

import (
        "bytes"
        "encoding/json"
        "fmt"
        "io"
        "path/filepath"
        "testing"

        "github.com/hashicorp/raft"
)

const (
        testKey       = "foo"
        testData      = "bar"
        backupName    = "backup.json"
        invalidBackup = "bad"
        stressEntries = 1024
        valueConst    = "val"
        keyPrefix     = "k"
)

func prepareStore(k, v string) *Store {
        s := New()
        b, _ := json.Marshal(Command{Op: OpPut, Key: S2B(k), Data: []byte(v)})
        s.Apply(&raft.Log{Data: b})
        return s
}

func TestBackupRestore(t *testing.T) {
        s := prepareStore(testKey, testData)
        var buf bytes.Buffer
        if err := s.Backup(&buf); err != nil {
                t.Fatalf("backup: %v", err)
        }
        s2 := New()
        if err := s2.RestoreBackup(&buf); err != nil {
                t.Fatalf("restore: %v", err)
        }
        v, ok := s2.Get(testKey)
        if !ok || string(v) != testData {
                t.Fatalf("mismatch got=%s ok=%v", v, ok)
        }
}

func TestBackupFileRestoreFile(t *testing.T) {
        dir := t.TempDir()
        path := filepath.Join(dir, backupName)
        s := prepareStore(testKey, testData)
        if err := s.BackupFile(path); err != nil {
                t.Fatalf("backupfile: %v", err)
        }
        s2 := New()
        if err := s2.RestoreBackupFile(path); err != nil {
                t.Fatalf("restorefile: %v", err)
        }
        v, ok := s2.Get(testKey)
        if !ok || string(v) != testData {
                t.Fatalf("mismatch got=%s ok=%v", v, ok)
        }
}

func TestBackupEmptyStore(t *testing.T) {
        s := New()
        var buf bytes.Buffer
        if err := s.Backup(&buf); err != nil {
                t.Fatalf("backup: %v", err)
        }
        s2 := New()
        if err := s2.RestoreBackup(&buf); err != nil {
                t.Fatalf("restore: %v", err)
        }
        if _, ok := s2.Get(testKey); ok {
                t.Fatalf("expected empty store")
        }
}

func TestRestoreBackupInvalid(t *testing.T) {
        s := New()
        buf := bytes.NewBufferString(invalidBackup)
        if err := s.RestoreBackup(buf); err == nil {
                t.Fatalf("expected error")
        }
}

func TestBackupStress(t *testing.T) {
        s := New()
        for i := 0; i < stressEntries; i++ {
                k := fmt.Sprintf("%s%d", keyPrefix, i)
                b, _ := json.Marshal(Command{Op: OpPut, Key: S2B(k), Data: []byte(valueConst)})
                s.Apply(&raft.Log{Data: b})
        }
        var buf bytes.Buffer
        if err := s.Backup(&buf); err != nil {
                t.Fatalf("backup: %v", err)
        }
        s2 := New()
        if err := s2.RestoreBackup(&buf); err != nil {
                t.Fatalf("restore: %v", err)
        }
        for i := 0; i < stressEntries; i++ {
                k := fmt.Sprintf("%s%d", keyPrefix, i)
                v, ok := s2.Get(k)
                if !ok || string(v) != valueConst {
                        t.Fatalf("missing %s", k)
                }
        }
}

func BenchmarkBackup(b *testing.B) {
        s := New()
        for i := 0; i < stressEntries; i++ {
                k := fmt.Sprintf("%s%d", keyPrefix, i)
                bts, _ := json.Marshal(Command{Op: OpPut, Key: S2B(k), Data: []byte(valueConst)})
                s.Apply(&raft.Log{Data: bts})
        }
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
                if err := s.Backup(io.Discard); err != nil {
                        b.Fatalf("backup: %v", err)
                }
        }
}

