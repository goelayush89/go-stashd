package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	stashd "github.com/goelayush89/go-stashd"
)

type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

var cache *stashd.Stash[string, User]

func init() {
	loader := stashd.LoaderFunc[string, User](func(key string) (User, error) {
		log.Printf("Loading user from database: %s", key)
		time.Sleep(100 * time.Millisecond)
		return User{ID: key, Name: "User " + key, Email: key + "@example.com"}, nil
	})

	suppressed := stashd.NewSuppressedLoader(loader)

	cfg := stashd.Config{
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		MaxEntries:      1000,
	}

	var err error
	cache, err = stashd.Open[string, User](cfg, stashd.WithLoader[string, User](suppressed))
	if err != nil {
		log.Fatal(err)
	}
}

func getUser(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("id")
	if userID == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	user, err := cache.GetOrLoad(userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(user)
}

func stats(w http.ResponseWriter, r *http.Request) {
	m := cache.Metrics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"hits":     m.Hits,
		"misses":   m.Misses,
		"hit_rate": fmt.Sprintf("%.2f%%", m.HitRate*100),
		"entries":  cache.Len(),
	})
}

func main() {
	http.HandleFunc("/user", getUser)
	http.HandleFunc("/stats", stats)

	log.Println("Starting API cache server on :8080")
	log.Println("  GET /user?id=123  - Get user (cached)")
	log.Println("  GET /stats        - Cache statistics")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
