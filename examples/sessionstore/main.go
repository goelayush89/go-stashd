package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"time"

	stashd "github.com/goelayush89/go-stashd"
)

type Session struct {
	UserID    string            `json:"user_id"`
	CreatedAt time.Time         `json:"created_at"`
	Data      map[string]string `json:"data"`
}

var sessions *stashd.Stash[string, Session]

func init() {
	events := stashd.EventHandlers[string, Session]{
		OnEviction: func(reason stashd.EvictionReason, sessionID string, session Session) {
			log.Printf("Session expired: %s (user: %s, reason: %s)", sessionID, session.UserID, reason)
		},
	}

	cfg := stashd.Config{
		DefaultTTL:      30 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		MaxEntries:      10000,
	}

	var err error
	sessions, err = stashd.Open[string, Session](cfg, stashd.WithEventHandlers[string, Session](events))
	if err != nil {
		log.Fatal(err)
	}
}

func generateSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func login(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user")
	if userID == "" {
		http.Error(w, "missing user", http.StatusBadRequest)
		return
	}

	sessionID := generateSessionID()
	session := Session{
		UserID:    userID,
		CreatedAt: time.Now(),
		Data:      make(map[string]string),
	}

	sessions.Put(sessionID, session)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"session_id": sessionID,
		"user_id":    userID,
	})
}

func getSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session")
	if sessionID == "" {
		http.Error(w, "missing session", http.StatusBadRequest)
		return
	}

	session, ok := sessions.Fetch(sessionID)
	if !ok {
		http.Error(w, "session not found or expired", http.StatusUnauthorized)
		return
	}

	sessions.Touch(sessionID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(session)
}

func logout(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session")
	if sessionID == "" {
		http.Error(w, "missing session", http.StatusBadRequest)
		return
	}

	removed, _ := sessions.Remove(sessionID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"removed": removed})
}

func stats(w http.ResponseWriter, r *http.Request) {
	m := sessions.Metrics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active_sessions": sessions.Len(),
		"total_created":   m.Puts,
		"expirations":     m.Evictions,
	})
}

func main() {
	http.HandleFunc("/login", login)
	http.HandleFunc("/session", getSession)
	http.HandleFunc("/logout", logout)
	http.HandleFunc("/stats", stats)

	log.Println("Starting session store on :8081")
	log.Println("  GET /login?user=alice    - Create session")
	log.Println("  GET /session?session=... - Get session (extends TTL)")
	log.Println("  GET /logout?session=...  - Delete session")
	log.Println("  GET /stats               - Store statistics")

	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatal(err)
	}
}
