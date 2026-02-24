[working-directory: "."]
run *ARGS:
    go run ./cmd/dendrite {{ARGS}}

lint:
    golangci-lint run ./...

up:
    docker compose up -d --wait

down:
    docker compose down

nuke:
    docker compose down --volumes --remove-orphans
