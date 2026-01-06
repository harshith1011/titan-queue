package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type Payment struct {
	ID             string
	Amount         int64
	Status         string
	IdempotencyKey string
}

type CreatePaymentRequest struct {
	Amount         int64  `json:"amount"`
	IdempotencyKey string `json:"idempotency_key"`
}

type CreatePaymentResponse struct {
	PaymentID string `json:"payment_id"`
	Amount    int64  `json:"amount"`
	Status    string `json:"status"`
}

// ---------- GLOBALS ----------
var db *sql.DB
var rdb *redis.Client
var ctx = context.Background()

// ---------- HELPERS ----------
func generatePaymentID() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("pay_%d", rand.Intn(1_000_000_000))
}

func connectDB() (*sql.DB, error) {
	connStr := "host=localhost port=5432 user=postgres password=harshith@123 dbname=payment_go sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return db, db.Ping()
}

// ---------- HANDLERS ----------

func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Payment API is running")
}

func getPaymentHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Path[len("/payments/"):]
	if id == "" {
		http.Error(w, "payment id is required", http.StatusBadRequest)
		return
	}

	var p Payment
	err := db.QueryRow(
		"SELECT id, amount, status FROM payments WHERE id=$1",
		id,
	).Scan(&p.ID, &p.Amount, &p.Status)

	if err == sql.ErrNoRows {
		http.Error(w, "payment not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	resp := CreatePaymentResponse{
		PaymentID: p.ID,
		Amount:    p.Amount,
		Status:    p.Status,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func payHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreatePaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Amount <= 0 || req.IdempotencyKey == "" {
		http.Error(w, "amount and idempotency_key are required", http.StatusBadRequest)
		return
	}

	paymentID := generatePaymentID()
	status := "PENDING"

	_, err := db.Exec(
		"INSERT INTO payments (id, amount, status, idempotency_key) VALUES ($1,$2,$3,$4)",
		paymentID, req.Amount, status, req.IdempotencyKey,
	)

	// ---------- IDEMPOTENCY ----------
	if err != nil {

		var existing Payment
		queryErr := db.QueryRow(
			"SELECT id, amount, status FROM payments WHERE idempotency_key=$1",
			req.IdempotencyKey,
		).Scan(&existing.ID, &existing.Amount, &existing.Status)

		if queryErr == nil {
			resp := CreatePaymentResponse{
				PaymentID: existing.ID,
				Amount:    existing.Amount,
				Status:    existing.Status,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}

		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	// ---------- ENQUEUE JOB TO REDIS ----------
	job := map[string]interface{}{
		"payment_id": paymentID,
	}

	jobBytes, _ := json.Marshal(job)

	// push to Redis list "jobs"
	rdb.LPush(ctx, "jobs", jobBytes)

	// mark QUEUED
	db.Exec("UPDATE payments SET status='QUEUED' WHERE id=$1", paymentID)

	// ---------- RESPONSE ----------
	resp := CreatePaymentResponse{
		PaymentID: paymentID,
		Amount:    req.Amount,
		Status:    "QUEUED",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ---------- MAIN ----------
func main() {

	var err error

	// DB
	db, err = connectDB()
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to PostgreSQL successfully!")

	// Redis
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	fmt.Println("Connected to Redis successfully!")

	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/pay", payHandler)
	http.HandleFunc("/payments/", getPaymentHandler)

	fmt.Println("Server started on :8080")
	http.ListenAndServe(":8080", nil)
}
