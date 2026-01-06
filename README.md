Titan Queue – Asynchronous Payment Processing System
Overview

Titan Queue is an asynchronous payment processing system built with:

Go (Golang)

Redis (as a job queue)

PostgreSQL (as primary database)

The system demonstrates how real-world payment gateways process payments reliably and safely, without blocking the user request.

Payments are not processed instantly. Instead, they are:

1. Created via API

2. Marked as PENDING

3. Enqueued into Redis

4. Picked by background workers

5.Status updated to:

   *PROCESSING

   *SUCCESS or FAILED

This project includes:

-> REST API service

-> Background worker service

-> Idempotency handling

-> Retry mechanism

-> Dead-letter queue support

-> Visibility timeout & recovery


Architecture:
            Client → Payment API → PostgreSQL
                   ↓
                Redis Queue
                   ↓
              Worker Service
                   ↓
            Updates Payment Status

Tech Stack

-> Go

-> PostgreSQL

-> Redis

-> HTTP REST API

-> SQL

Payment Status Flow
         PENDING  →  QUEUED  →  PROCESSING  →  SUCCESS
                                   →  FAILED



API Endpoints:
Create Payment

POST /pay

Request:

{
  "amount": 500,
  "idempotency_key": "abc123"
}


Response:
{
  "payment_id": "pay_12345",
  "amount": 500,
  "status": "PENDING"
}

Get Payment Status:

GET /payments/{payment_id}

Example:

GET /payments/pay_12345


Response:

{
  "payment_id": "pay_12345",
  "amount": 500,
  "status": "SUCCESS"
}

Worker Behavior

The worker:

-> pulls jobs from Redis

-> marks job as PROCESSING

-> simulates payment gateway delay

-> randomly succeeds or fails

-> retries up to N times

-> moves to DLQ after max retries

-> updates PostgreSQL status

-> marks processed IDs to avoid duplicates

Queues used:
jobs          → pending jobs
processing    → in-flight jobs
dlq           → dead letter failed jobs
processed:*   → idempotency keys


Running the Project:
Requirements

Go installed

Redis running locally

PostgreSQL running locally


Database Schema:

Create PostgreSQL database:

CREATE TABLE payments(
    id TEXT PRIMARY KEY,
    amount BIGINT NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT UNIQUE NOT NULL
);


**How to run
->Start API server
->go run api/main.go

Start Worker
->go run worker/main.go

What this project demonstrates:

real-world payment system workflow

asynchronous job execution

database + cache interaction

backend reliability patterns

message queue design

idempotency handling

worker system design

Future Enhancements

-> webhook notifications

-> UI dashboard

-> docker-compose

-> exponential backoff retries

-> monitoring

-> Prometheus metrics

-> multiple queues by priority

-> actual payment gateways

Author
Polavarapu Harshith Vardhan