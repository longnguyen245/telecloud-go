package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"telecloud/config"
	"telecloud/database"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHandleDeleteUserCleanup(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Initialize sqlite file DB
	tempDB := "test_temp.db"
	defer func() {
		if database.RWDB != nil {
			database.RWDB.Close()
		}
		if database.RODB != nil {
			database.RODB.Close()
		}
		os.Remove(tempDB)
		os.Remove(tempDB + "-shm")
		os.Remove(tempDB + "-wal")
	}()

	err := database.InitDB("sqlite", tempDB, "")
	if err != nil {
		t.Fatalf("failed to initialize db: %v", err)
	}

	// Prepare data
	username := "testchild"
	adminUser := "admin"

	// 1. Insert admin username to settings (for path backfill / owner rename logic)
	_, err = database.DB.Exec("INSERT INTO settings (key, value) VALUES ('admin_username', ?)", adminUser)
	if err != nil {
		t.Fatalf("failed to setup admin settings: %v", err)
	}

	// 2. Insert child account
	_, err = database.DB.Exec("INSERT INTO child_accounts (username, password_hash) VALUES (?, 'some_hash')", username)
	if err != nil {
		t.Fatalf("failed to insert child_account: %v", err)
	}

	// 3. Insert file (root user folder)
	_, err = database.DB.Exec("INSERT INTO files (filename, path, is_folder, owner) VALUES (?, '/', TRUE, ?)", username, username)
	if err != nil {
		t.Fatalf("failed to insert folder file: %v", err)
	}

	// 4. Insert user settings
	_, err = database.DB.Exec("INSERT INTO user_settings (username, key, value) VALUES (?, 'telegram_user_id', '123456789')", username)
	if err != nil {
		t.Fatalf("failed to insert user_settings: %v", err)
	}
	_, err = database.DB.Exec("INSERT INTO user_settings (username, key, value) VALUES (?, 'bot_pool_upload_folder', 'TelegramUpload')", username)
	if err != nil {
		t.Fatalf("failed to insert user_settings: %v", err)
	}

	// 5. Insert upload task and chunks
	taskID := "task-123"
	_, err = database.DB.Exec("INSERT INTO upload_tasks (id, filename, owner) VALUES (?, 'file.txt', ?)", taskID, username)
	if err != nil {
		t.Fatalf("failed to insert upload task: %v", err)
	}
	_, err = database.DB.Exec("INSERT INTO upload_chunks (task_id, chunk_index) VALUES (?, 0)", taskID)
	if err != nil {
		t.Fatalf("failed to insert upload chunk: %v", err)
	}

	// 6. Insert session
	_, err = database.DB.Exec("INSERT INTO sessions (token, username) VALUES ('sess-tok-123', ?)", username)
	if err != nil {
		t.Fatalf("failed to insert session: %v", err)
	}

	// 7. Insert passkey
	_, err = database.DB.Exec("INSERT INTO passkeys (username, credential_id, public_key) VALUES (?, 'cred_id_123', 'pub_key_123')", username)
	if err != nil {
		t.Fatalf("failed to insert passkey: %v", err)
	}

	// Set up gin router
	h := &Handler{
		cfg: &config.Config{
			Version: "1.0",
		},
	}
	r := gin.New()
	r.DELETE("/api/users/:username", func(c *gin.Context) {
		c.Set("is_admin", true)
		c.Set("username", adminUser)
		h.handleDeleteUser(c)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/api/users/"+username, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}
	if resp["status"] != "success" {
		t.Fatalf("expected status 'success', got %v", resp["status"])
	}

	// Verification: Child account must be deleted
	var count int
	database.RODB.Get(&count, "SELECT COUNT(*) FROM child_accounts WHERE username = ?", username)
	if count != 0 {
		t.Errorf("expected child_accounts count to be 0, got %d", count)
	}

	// Verification: User settings must be deleted
	database.RODB.Get(&count, "SELECT COUNT(*) FROM user_settings WHERE username = ?", username)
	if count != 0 {
		t.Errorf("expected user_settings count to be 0, got %d", count)
	}

	// Verification: Upload tasks must be deleted
	database.RODB.Get(&count, "SELECT COUNT(*) FROM upload_tasks WHERE owner = ?", username)
	if count != 0 {
		t.Errorf("expected upload_tasks count to be 0, got %d", count)
	}

	// Verification: Upload chunks must be deleted
	database.RODB.Get(&count, "SELECT COUNT(*) FROM upload_chunks WHERE task_id = ?", taskID)
	if count != 0 {
		t.Errorf("expected upload_chunks count to be 0, got %d", count)
	}

	// Verification: Sessions must be deleted
	database.RODB.Get(&count, "SELECT COUNT(*) FROM sessions WHERE username = ?", username)
	if count != 0 {
		t.Errorf("expected sessions count to be 0, got %d", count)
	}

	// Verification: Passkeys must be deleted
	database.RODB.Get(&count, "SELECT COUNT(*) FROM passkeys WHERE username = ?", username)
	if count != 0 {
		t.Errorf("expected passkeys count to be 0, got %d", count)
	}

	// Verification: User files/folders must be renamed and owned by admin
	var folderOwner string
	err = database.RODB.Get(&folderOwner, "SELECT owner FROM files WHERE filename LIKE 'deleted_testchild_%' AND path = '/'")
	if err != nil {
		t.Errorf("failed to retrieve deleted user folder file record: %v", err)
	} else if folderOwner != adminUser {
		t.Errorf("expected deleted user folder owner to be %q, got %q", adminUser, folderOwner)
	}
}
