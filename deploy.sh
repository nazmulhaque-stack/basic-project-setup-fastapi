#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh — one-command production setup and launch.
#
# What it does, in order:
#   1. Installs Docker Engine + the Docker Compose plugin if either is missing
#      (apt/dnf/yum/pacman auto-detected; falls back to Docker's official
#      install script otherwise).
#   2. Creates .env from .env.example on first run, and replaces every secret
#      still left at its example placeholder value with a freshly generated
#      random one (DB_PASSWORD, REDIS_PASSWORD, ELASTICSEARCH_PASSWORD,
#      GRAFANA_ADMIN_PASSWORD, ENCRYPTION_KEYS, BLIND_INDEX_KEY) — never
#      touches a value you've already customized.
#   3. Builds and starts the core stack (Postgres, PgBouncer, Redis 7,
#      Elasticsearch 8, Nginx, and the FastAPI app itself via Gunicorn) with
#      deploy/docker-compose.yml.
#   4. Waits for the app's /healthz and /readyz endpoints, then prints every
#      service's URL (ports read straight from .env).
#
# Usage:
#   ./deploy.sh              Full setup + build + start (safe to re-run)
#   ./deploy.sh --rebuild    Force a clean image rebuild (--no-cache) first
#   ./deploy.sh --verify     Live-check every service (DB, PgBouncer, Redis,
#                            Elasticsearch, Nginx, app, k8s manifests)
#                            and print PASS/FAIL for each
#   ./deploy.sh --logs       Follow the app container's logs
#   ./deploy.sh --status     Show container status
#   ./deploy.sh --down       Stop and remove all containers (keeps volumes)
#   ./deploy.sh --down -v    Stop and remove all containers AND data volumes
#   ./deploy.sh --help       Show this help
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_FILE="deploy/docker-compose.yml"
ENV_FILE=".env"
ENV_EXAMPLE=".env.example"

# ── Output helpers ────────────────────────────────────────────────────────────
c_info()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$1"; }
c_ok()    { printf '\033[1;32m[ OK ]\033[0m  %s\n' "$1"; }
c_warn()  { printf '\033[1;33m[WARN]\033[0m  %s\n' "$1"; }
c_error() { printf '\033[1;31m[FAIL]\033[0m  %s\n' "$1" >&2; }

# ── docker compose wrapper: always targets the compose file + env file above,
#    and is invoked from the repo root so `--env-file .env` substitution and
#    the compose file's own `env_file: ../.env` directives both resolve to
#    the exact same file ─────────────────────────────────────────────────────
dc() {
    local sudo_prefix=""
    [ "$NEEDS_SUDO" = "1" ] && sudo_prefix="sudo"
    $sudo_prefix docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

usage() {
    sed -n '2,31p' "$0" | sed 's/^# \{0,1\}//'
}

# ── 1. Docker / Docker Compose installation ──────────────────────────────────
detect_pkg_manager() {
    if command -v apt-get >/dev/null 2>&1; then echo "apt"
    elif command -v dnf >/dev/null 2>&1; then echo "dnf"
    elif command -v yum >/dev/null 2>&1; then echo "yum"
    elif command -v pacman >/dev/null 2>&1; then echo "pacman"
    else echo "unknown"
    fi
}

install_docker() {
    c_warn "Docker is not installed. Installing it now..."
    local pkg_mgr
    pkg_mgr="$(detect_pkg_manager)"

    case "$pkg_mgr" in
        apt)
            sudo apt-get update -y
            sudo apt-get install -y ca-certificates curl gnupg
            sudo install -m 0755 -d /etc/apt/keyrings
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
                | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
            sudo chmod a+r /etc/apt/keyrings/docker.gpg
            # shellcheck disable=SC1091
            . /etc/os-release
            echo \
              "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME:-stable} stable" \
              | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            sudo apt-get update -y
            sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        dnf)
            sudo dnf -y install dnf-plugins-core
            sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
            sudo dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        yum)
            sudo yum install -y yum-utils
            sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
            sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        pacman)
            sudo pacman -Sy --noconfirm docker docker-compose
            ;;
        *)
            c_warn "Unrecognized package manager; falling back to Docker's official install script."
            curl -fsSL https://get.docker.com | sudo sh
            ;;
    esac

    sudo systemctl enable docker --now 2>/dev/null || sudo service docker start || true
    c_ok "Docker installed."

    if [ -n "${SUDO_USER:-}" ] || [ "$(id -u)" -ne 0 ]; then
        local target_user="${SUDO_USER:-$USER}"
        sudo usermod -aG docker "$target_user" || true
        c_warn "Added '$target_user' to the 'docker' group. Log out and back in (or run 'newgrp docker') to use docker without sudo. Continuing this run with sudo."
        NEEDS_SUDO=1
    fi
}

ensure_docker() {
    NEEDS_SUDO=0
    if ! command -v docker >/dev/null 2>&1; then
        install_docker
    elif ! docker info >/dev/null 2>&1; then
        if sudo -n docker info >/dev/null 2>&1; then
            NEEDS_SUDO=1
            c_warn "Current user can't run Docker directly; using sudo for docker commands."
        else
            c_warn "Docker is installed but not usable by this user yet (permission or daemon issue)."
            c_info "Trying 'sudo docker info'..."
            if sudo docker info >/dev/null 2>&1; then
                NEEDS_SUDO=1
            else
                c_error "Docker daemon isn't reachable even with sudo. Is the docker service running? Try: sudo systemctl start docker"
                exit 1
            fi
        fi
    fi
    c_ok "Docker is available."

    if ! docker compose version >/dev/null 2>&1 && ! sudo docker compose version >/dev/null 2>&1; then
        c_warn "Docker Compose plugin missing; installing it."
        local pkg_mgr
        pkg_mgr="$(detect_pkg_manager)"
        case "$pkg_mgr" in
            apt) sudo apt-get update -y && sudo apt-get install -y docker-compose-plugin ;;
            dnf) sudo dnf -y install docker-compose-plugin ;;
            yum) sudo yum install -y docker-compose-plugin ;;
            pacman) sudo pacman -Sy --noconfirm docker-compose ;;
            *) c_error "Could not auto-install the Docker Compose plugin on this OS. Install it manually: https://docs.docker.com/compose/install/"; exit 1 ;;
        esac
    fi
    c_ok "Docker Compose is available."
}

# ── 2. .env setup ─────────────────────────────────────────────────────────────
# Replaces a KEY=value line in .env, only touching that one line.
set_env_var() {
    local key="$1" value="$2"
    local escaped_value
    escaped_value=$(printf '%s' "$value" | sed -e 's/[&/\]/\\&/g')
    if grep -q "^${key}=" "$ENV_FILE"; then
        sed -i "s|^${key}=.*|${key}=${escaped_value}|" "$ENV_FILE"
    else
        printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
    fi
}

get_env_var() {
    local key="$1" default="$2"
    local line
    line=$(grep "^${key}=" "$ENV_FILE" 2>/dev/null | tail -n1 | cut -d'=' -f2- || true)
    [ -n "$line" ] && printf '%s' "$line" || printf '%s' "$default"
}

ensure_env_file() {
    if [ ! -f "$ENV_FILE" ]; then
        if [ ! -f "$ENV_EXAMPLE" ]; then
            c_error "$ENV_EXAMPLE not found — can't create $ENV_FILE."
            exit 1
        fi
        cp "$ENV_EXAMPLE" "$ENV_FILE"
        c_ok "Created $ENV_FILE from $ENV_EXAMPLE."
    else
        c_info "$ENV_FILE already exists — only replacing secrets still left at their example placeholder."
    fi

    # Only replace a secret if it's still exactly the placeholder shipped in
    # .env.example — never overwrites a value you set yourself.
    if [ "$(get_env_var DB_PASSWORD '')" = "change-me-before-production" ]; then
        set_env_var DB_PASSWORD "$(openssl rand -hex 24)"
        c_ok "Generated a random DB_PASSWORD."
    fi
    if [ "$(get_env_var GRAFANA_ADMIN_PASSWORD '')" = "change-me-before-production" ]; then
        set_env_var GRAFANA_ADMIN_PASSWORD "$(openssl rand -hex 16)"
        c_ok "Generated a random GRAFANA_ADMIN_PASSWORD."
    fi
    if [ -z "$(get_env_var ELASTICSEARCH_PASSWORD '')" ]; then
        set_env_var ELASTICSEARCH_PASSWORD "$(openssl rand -hex 16)"
        c_ok "Generated a random ELASTICSEARCH_PASSWORD."
    fi
    if [ -z "$(get_env_var REDIS_PASSWORD '')" ]; then
        set_env_var REDIS_PASSWORD "$(openssl rand -hex 16)"
        c_ok "Generated a random REDIS_PASSWORD."
    fi
    local enc_keys
    enc_keys="$(get_env_var ENCRYPTION_KEYS '')"
    if [ "$enc_keys" = '{"1": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}' ]; then
        set_env_var ENCRYPTION_KEYS "{\"1\": \"$(openssl rand -hex 32)\"}"
        c_ok "Generated a random ENCRYPTION_KEYS (version 1)."
    fi
    if [ "$(get_env_var BLIND_INDEX_KEY '')" = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210" ]; then
        set_env_var BLIND_INDEX_KEY "$(openssl rand -hex 32)"
        c_ok "Generated a random BLIND_INDEX_KEY."
    fi

    # Keep DATABASE_URL/MIGRATION_DATABASE_URL's embedded credentials in sync
    # with DB_USER/DB_PASSWORD/DB_NAME above (app/core/config.py does the same
    # sync for the running app; this just keeps the .env file itself readable
    # and consistent for anyone who greps it directly).
    local db_user db_password db_name
    db_user="$(get_env_var DB_USER appuser)"
    db_password="$(get_env_var DB_PASSWORD change-me-before-production)"
    db_name="$(get_env_var DB_NAME app_db)"
    set_env_var DATABASE_URL "postgresql://${db_user}:${db_password}@127.0.0.1:$(get_env_var PGBOUNCER_PORT 6432)/${db_name}"
    set_env_var MIGRATION_DATABASE_URL "postgresql://${db_user}:${db_password}@127.0.0.1:$(get_env_var POSTGRES_PORT 5432)/${db_name}"

    # Same sync for REDIS_URL (app/core/redis.py connects via this directly,
    # not the discrete REDIS_USER/REDIS_PASSWORD fields, whenever it's set).
    local redis_user redis_password
    redis_user="$(get_env_var REDIS_USER default)"
    redis_password="$(get_env_var REDIS_PASSWORD '')"
    set_env_var REDIS_URL "redis://${redis_user}:${redis_password}@127.0.0.1:$(get_env_var REDIS_PORT 6379)/0"

    chmod 600 "$ENV_FILE" || true
}

# ── 3/4. Build, start, wait, summarize ───────────────────────────────────────
wait_for_app() {
    local app_port
    app_port="$(get_env_var APP_PORT 8000)"
    c_info "Waiting for the app to become healthy on port ${app_port}..."
    local attempt=0
    until curl -sf "http://127.0.0.1:${app_port}/healthz" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 90 ]; then
            c_error "App did not become healthy within 90s. Check logs: ./deploy.sh --logs"
            exit 1
        fi
        sleep 1
    done
    c_ok "App is alive (/healthz)."

    attempt=0
    until curl -sf "http://127.0.0.1:${app_port}/readyz" >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 60 ]; then
            c_warn "App is alive but /readyz hasn't passed yet (Postgres/Redis/Elasticsearch may still be settling). Check: ./deploy.sh --logs"
            return
        fi
        sleep 1
    done
    c_ok "App is ready (/readyz) — Postgres, Redis, and Elasticsearch are all reachable."
}

print_summary() {
    c_info "─────────────────────────────────────────────────────────────"
    c_info "Service URLs (ports read from $ENV_FILE):"
    printf '  %-28s http://127.0.0.1:%s\n' "App (direct)"        "$(get_env_var APP_PORT 8000)"
    printf '  %-28s http://127.0.0.1:%s\n' "App (via Nginx)"     "$(get_env_var NGINX_PORT 80)"
    printf '  %-28s http://127.0.0.1:%s/docs\n' "API docs"       "$(get_env_var APP_PORT 8000)"
    printf '  %-28s127.0.0.1:%s\n' "Postgres (direct)"           "$(get_env_var POSTGRES_PORT 5432)"
    printf '  %-28s127.0.0.1:%s\n' "PgBouncer (app uses this)"   "$(get_env_var PGBOUNCER_PORT 6432)"
    printf '  %-28shttp://127.0.0.1:%s\n' "Elasticsearch"        "$(get_env_var ELASTICSEARCH_PORT 9200)"
    c_info "─────────────────────────────────────────────────────────────"
    c_info "Logs:    ./deploy.sh --logs"
    c_info "Status:  ./deploy.sh --status"
    c_info "Stop:    ./deploy.sh --down"
}

run_setup() {
    local rebuild="${1:-0}"
    ensure_docker
    ensure_env_file

    c_info "Building images..."
    if [ "$rebuild" = "1" ]; then
        dc build --no-cache
    else
        dc build
    fi

    c_info "Starting the core stack (Postgres, PgBouncer, Redis, Elasticsearch, Nginx, app)..."
    dc up -d

    wait_for_app
    print_summary
}

# ── Live verification of every component ────────────────────────────────────
# Functional checks (everything is up, reachable, and answering correctly) —
# not a load test. See the note printed at the end for how to actually test
# 100k-concurrency behavior.
verify_stack() {
    ensure_docker
    [ -f "$ENV_FILE" ] || { c_error "$ENV_FILE not found — run './deploy.sh' first to set up and start the stack."; exit 1; }

    local overall_ok=1
    local app_port nginx_port es_port pgbouncer_port db_user db_name
    app_port="$(get_env_var APP_PORT 8000)"
    nginx_port="$(get_env_var NGINX_PORT 80)"
    es_port="$(get_env_var ELASTICSEARCH_PORT 9200)"
    pgbouncer_port="$(get_env_var PGBOUNCER_PORT 6432)"
    db_user="$(get_env_var DB_USER postgres)"
    db_name="$(get_env_var DB_NAME app_db)"

    check() {
        local label="$1"; shift
        if "$@" >/dev/null 2>&1; then
            printf '  \033[1;32m\xe2\x9c\x93 PASS\033[0m  %s\n' "$label"
        else
            printf '  \033[1;31m\xe2\x9c\x97 FAIL\033[0m  %s\n' "$label"
            overall_ok=0
        fi
    }

    readyz_component() {
        # Parses /readyz's JSON and checks one boolean field under "components"
        # — robust to whichever whitespace style the JSON encoder used.
        curl -sf "http://127.0.0.1:${app_port}/readyz" \
            | python3 -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get('components',{}).get('$1') else 1)"
    }

    c_info "Verifying every service in the stack. This checks reachability and health, not load capacity."
    echo

    echo "── Database (Postgres + PgBouncer) ─────────────────────────"
    check "Postgres accepting connections"              docker exec demo_postgres pg_isready -U "$db_user" -d "$db_name"
    check "PgBouncer port reachable"                     bash -c "exec 3<>/dev/tcp/127.0.0.1/${pgbouncer_port}"
    check "App reaches the DB through PgBouncer (/readyz)" readyz_component database

    echo
    echo "── Redis 7 ────────────────────────────────────────────────────"
    local redis_password
    redis_password="$(get_env_var REDIS_PASSWORD '')"
    check "Redis responds to PING"                        docker exec demo_redis redis-cli -a "$redis_password" --no-auth-warning ping
    check "App reaches Redis (/readyz)"                   readyz_component redis

    echo
    echo "── Elasticsearch 8 ───────────────────────────────────────────"
    check "Cluster health endpoint reachable"             curl -sf "http://127.0.0.1:${es_port}/_cluster/health"
    check "Cluster status is not red"                     bash -c "curl -sf http://127.0.0.1:${es_port}/_cluster/health | grep -qv '\"status\":\"red\"'"

    echo
    echo "── Application & Nginx ─────────────────────────────────────────"
    check "App /healthz (liveness)"                       curl -sf "http://127.0.0.1:${app_port}/healthz"
    check "App /readyz (DB + Redis + Elasticsearch all reachable)" curl -sf "http://127.0.0.1:${app_port}/readyz"
    check "Nginx proxies /healthz through to the app"      curl -sf "http://127.0.0.1:${nginx_port}/healthz"

    echo
    echo "── Kubernetes manifests (deploy/k8s/) ──────────────────────────"
    local k8s_file
    for k8s_file in deploy/k8s/*.yaml; do
        check "$(basename "$k8s_file") is valid YAML"    python3 -c "import yaml; yaml.safe_load(open('$k8s_file'))"
    done
    if command -v kubectl >/dev/null 2>&1 && kubectl cluster-info >/dev/null 2>&1; then
        check "Manifests pass a live server-side dry run" kubectl apply --dry-run=server -f deploy/k8s/
    else
        c_warn "No live Kubernetes cluster reachable (kubectl not installed/configured) — only validated YAML syntax above, nothing was applied to a cluster."
    fi

    echo
    if [ "$overall_ok" = "1" ]; then
        c_ok "All checks passed — Postgres, PgBouncer, Redis, Elasticsearch, the app, and Nginx are all healthy end to end."
    else
        c_error "One or more checks failed (see the FAIL lines above). Run './deploy.sh --logs' or 'docker compose -f $COMPOSE_FILE logs <service>' to investigate."
    fi
    echo
    c_info "Note: this confirms every component is reachable and healthy — it is NOT a load"
    c_info "test. Verifying the architecture actually sustains 100,000 (1 lakh) concurrent"
    c_info "requests needs a real load-testing tool (k6, hey, or Locust) driven from a"
    c_info "separate machine against the Nginx port. Ask if you'd like that set up."
    [ "$overall_ok" = "1" ]
}

# ── Entry point ───────────────────────────────────────────────────────────────
case "${1:-}" in
    --help|-h)
        usage
        ;;
    --down)
        ensure_docker
        if [ "${2:-}" = "-v" ]; then
            c_warn "Removing containers AND data volumes (Postgres/Redis/Elasticsearch data will be lost)."
            dc down -v
        else
            dc down
        fi
        c_ok "Stack stopped."
        ;;
    --logs)
        ensure_docker
        dc logs -f app
        ;;
    --status)
        ensure_docker
        dc ps
        ;;
    --rebuild)
        run_setup 1
        ;;
    --verify)
        verify_stack
        ;;
    "")
        run_setup 0
        ;;
    *)
        c_error "Unknown option: $1"
        usage
        exit 1
        ;;
esac
