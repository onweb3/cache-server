package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net/http"
	"time"
)

// CacheEntry represents a single entry in the cache
type CacheEntry struct {
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
	ExpireAt time.Time   `json:"expireAt"`
}

var redisClient *redis.Client

func main() {
	// Replace with your actual Redis connection details
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // Add password if required
		DB:       0,  // Select DB (optional)
	})

	ctx := context.Background()
	pong, err := redisClient.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Error connecting to Redis:", err)
		return
	}
	fmt.Println("Connected to Redis:", pong)

	http.HandleFunc("/get", getCache)
	http.HandleFunc("/set", setCache)

	fmt.Println("Cache server listening on port 8080")
	http.ListenAndServe(":8080", nil)
}

func getCache(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	val, err := redisClient.Get(ctx, key).Result()
	if err == redis.Nil { // Key not found
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Error retrieving data from cache", http.StatusInternalServerError)
		return
	}

	var entry CacheEntry
	err = json.Unmarshal([]byte(val), &entry)
	if err != nil {
		http.Error(w, "Invalid data format in cache", http.StatusInternalServerError)
		return
	}

	if time.Now().After(entry.ExpireAt) {
		// Key found but expired, remove it and return not found
		delCmd := redisClient.Del(ctx, key)
		if err := delCmd.Err(); err != nil {
			fmt.Println("Error deleting expired key:", err)
		}
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry.Value)
}

func setCache(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	var value interface{}
	err := json.NewDecoder(r.Body).Decode(&value)
	if err != nil {
		http.Error(w, "Invalid JSON data", http.StatusBadRequest)
		return
	}

	expire, err := time.ParseDuration(r.URL.Query().Get("expire"))
	if err != nil {
		expire = 0 // No expiration by default
	}

	entry := CacheEntry{
		Key:      key,
		Value:    value,
		ExpireAt: time.Now().Add(expire),
	}

	marshalled, err := json.Marshal(entry)
	if err != nil {
		http.Error(w, "Error marshalling data", http.StatusInternalServerError)
		return
	}

	ctx := context.Background()
	_, err = redisClient.Set(ctx, key, marshalled, expire).Result()
	if err != nil {
		http.Error(w, "Error setting data in cache", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
