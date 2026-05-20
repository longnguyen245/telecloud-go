package api

import (
	"net/http"
	"strconv"
	"strings"
	"telecloud/database"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

func (h *Handler) handleGetLogin(c *gin.Context) {
	adminUser := database.GetSetting("admin_username")
	if adminUser == "" {
		c.Redirect(http.StatusFound, "/setup")
		return
	}

	token, _ := c.Cookie("session_token")
	sessionUsername := database.LookupSessionUser(token)
	if sessionUsername != "" {
		c.Redirect(http.StatusFound, "/")
		return
	}
	setCSRFCookie(c)
	c.HTML(http.StatusOK, "login.html", gin.H{
		"version": h.cfg.Version,
	})
}

// bumpAttempt records a failed authentication attempt against the given IP.
// Shared by /login and /setup so a determined attacker can't trivially burn
// attempts on one endpoint and switch to the other.
func bumpAttempt(ip string) {
	v, _ := loginAttempts.Load(ip)
	var att loginAttempt
	if v != nil {
		att = v.(loginAttempt)
	}
	att.count++
	att.last = time.Now()
	loginAttempts.Store(ip, att)
}

func (h *Handler) handlePostLogin(c *gin.Context) {
	ip := c.ClientIP()
	val, _ := loginAttempts.Load(ip)
	var att loginAttempt
	if val != nil {
		att = val.(loginAttempt)
		if att.count >= 5 && time.Since(att.last) < 15*time.Minute {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "too_many_requests"})
			return
		}
	}

	username := c.PostForm("username")
	password := c.PostForm("password")

	dbUser := database.GetSetting("admin_username")
	dbHash := database.GetSetting("admin_password_hash")

	var authSuccess bool
	var forceChange bool
	if username == dbUser && bcrypt.CompareHashAndPassword([]byte(dbHash), []byte(password)) == nil {
		authSuccess = true
	} else {
		var child struct {
			Hash        string `db:"password_hash"`
			ForceChange int    `db:"force_password_change"`
		}
		err := database.RODB.Get(&child, "SELECT password_hash, force_password_change FROM child_accounts WHERE username = ?", username)
		if err == nil && bcrypt.CompareHashAndPassword([]byte(child.Hash), []byte(password)) == nil {
			authSuccess = true
			forceChange = child.ForceChange == 1
		}
	}

	if authSuccess {
		if forceChange {
			c.JSON(http.StatusOK, gin.H{"status": "force_password_change", "username": username})
			return
		}
		loginAttempts.Delete(ip) // Reset on success
		sessionToken, err := database.CreateSession(username)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create session"})
			return
		}
		c.SetCookie("session_token", sessionToken, int(database.SessionTTL.Seconds()), "/", "", isSecure(), true)
		database.LogAuditFromCtx(c, username, database.AuditActionLoginSuccess, "", database.AuditStatusOK)
		c.JSON(http.StatusOK, gin.H{"status": "success"})
		return
	}

	// On failure
	att.count++
	att.last = time.Now()
	loginAttempts.Store(ip, att)

	// Artificial delay to thwart fast scripts
	time.Sleep(1 * time.Second)

	database.LogAuditFromCtx(c, username, database.AuditActionLoginFail, "", database.AuditStatusDenied)
	if att.count >= 5 {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "ip_blocked"})
	} else {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
	}
}

func (h *Handler) handleLogout(c *gin.Context) {
	token, _ := c.Cookie("session_token")
	actor := database.LookupSessionUser(token)
	if token != "" {
		database.DB.Exec("DELETE FROM sessions WHERE token = ?", token)
	}
	c.SetCookie("session_token", "", -1, "/", "", isSecure(), true)
	c.SetCookie(csrfCookieName, "", -1, "/", "", isSecure(), false)
	if actor != "" {
		database.LogAuditFromCtx(c, actor, database.AuditActionLogout, "", database.AuditStatusOK)
	}
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func (h *Handler) handleGetResetAdmin(c *gin.Context) {
	token := strings.TrimSpace(c.Query("token"))
	dbToken := strings.TrimSpace(database.GetSetting("admin_reset_token"))
	expiryStr := strings.TrimSpace(database.GetSetting("admin_reset_expiry"))

	if token == "" || token != dbToken {
		c.String(http.StatusForbidden, "Invalid token")
		return
	}

	expiry, _ := strconv.ParseInt(expiryStr, 10, 64)
	if time.Now().Unix() > expiry {
		c.String(http.StatusForbidden, "Token expired")
		return
	}

	setCSRFCookie(c)
	c.HTML(http.StatusOK, "reset-admin.html", gin.H{
		"version": h.cfg.Version,
	})
}

func (h *Handler) handlePostResetAdmin(c *gin.Context) {
	token := strings.TrimSpace(c.PostForm("token"))
	if token == "" {
		token = strings.TrimSpace(c.Query("token"))
	}
	password := c.PostForm("password")

	dbToken := strings.TrimSpace(database.GetSetting("admin_reset_token"))
	expiryStr := strings.TrimSpace(database.GetSetting("admin_reset_expiry"))

	if token == "" || token != dbToken {
		c.JSON(http.StatusForbidden, gin.H{"error": "invalid_token"})
		return
	}

	expiry, _ := strconv.ParseInt(expiryStr, 10, 64)
	if time.Now().Unix() > expiry {
		c.JSON(http.StatusForbidden, gin.H{"error": "token_expired"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed_to_hash_password"})
		return
	}

	database.SetSetting("admin_password_hash", string(hashedPassword))
	database.DeleteSetting("admin_reset_token")
	database.DeleteSetting("admin_reset_expiry")

	// Clear admin sessions
	adminUser := database.GetSetting("admin_username")
	if adminUser != "" {
		database.DB.Exec("DELETE FROM sessions WHERE username = ?", adminUser)
	}
	database.LogAuditFromCtx(c, adminUser, database.AuditActionAdminReset, "", database.AuditStatusOK)

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}
