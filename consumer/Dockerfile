FROM golang:1.23.4-alpine AS builder

# Set up application directory
WORKDIR /app

# Copy Go modules and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the application source code
COPY . .

# Build the application
RUN go build -o main .

# Create a runtime image
FROM alpine:latest AS runtime

# Set up application directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Run the application
CMD ["./main"]