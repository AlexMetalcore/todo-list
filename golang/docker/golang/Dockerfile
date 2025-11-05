FROM golang:alpine AS builder

WORKDIR /app

COPY golang/go.mod ./
RUN go mod download

COPY /golang .