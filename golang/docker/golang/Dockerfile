FROM golang:alpine AS builder

WORKDIR /app

COPY golang/go.mod ./
COPY golang/go.sum ./
RUN go mod download

COPY /golang .

RUN go build -o todolist .

EXPOSE 8089