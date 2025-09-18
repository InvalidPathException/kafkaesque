# ---- Asset build stage ----
FROM node:20-slim AS assets

WORKDIR /app

# Copy package files
COPY apps/kafkaesque_dashboard/assets/package*.json ./

# Install npm dependencies
RUN npm ci --production=false

# Copy asset sources
COPY apps/kafkaesque_dashboard/assets ./

# Build assets for production
RUN npm run build

# ---- Build image ----
FROM elixir:1.18-slim AS build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install hex and rebar
RUN mix do local.hex --force, local.rebar --force

# Copy umbrella dependencies
COPY mix.exs mix.lock ./
COPY config config

# Copy each app's mix.exs
COPY apps/kafkaesque_core/mix.exs apps/kafkaesque_core/
COPY apps/kafkaesque_server/mix.exs apps/kafkaesque_server/
COPY apps/kafkaesque_dashboard/mix.exs apps/kafkaesque_dashboard/
COPY apps/kafkaesque_proto/mix.exs apps/kafkaesque_proto/
COPY apps/kafkaesque_client/mix.exs apps/kafkaesque_client/
COPY apps/kafkaesque_test_support/mix.exs apps/kafkaesque_test_support/

# Get and compile dependencies
ENV MIX_ENV=prod
RUN mix deps.get --only prod
RUN mix deps.compile

# Copy application code
COPY apps/kafkaesque_core apps/kafkaesque_core
COPY apps/kafkaesque_server apps/kafkaesque_server
COPY apps/kafkaesque_dashboard apps/kafkaesque_dashboard
COPY apps/kafkaesque_proto apps/kafkaesque_proto
COPY apps/kafkaesque_client apps/kafkaesque_client
COPY apps/kafkaesque_test_support apps/kafkaesque_test_support
COPY proto proto

# Copy built assets from assets stage
COPY --from=assets /app/../priv/static/assets apps/kafkaesque_dashboard/priv/static/assets

# Compile the application
RUN mix compile

# Digest static files
RUN mix phx.digest

# Build the release
RUN mix release kafkaesque

# ---- Runtime image ----
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssl \
    curl \
    ca-certificates \
    bash \
    tzdata \
  && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -ms /bin/bash app
WORKDIR /home/app

# Create data directories
RUN mkdir -p /var/lib/kafkaesque/data /var/lib/kafkaesque/offsets
RUN chown -R app:app /var/lib/kafkaesque

# Set environment variables
ENV LANG=C.UTF-8 \
    HOME=/home/app \
    MIX_ENV=prod \
    PHX_SERVER=true \
    PORT=4000 \
    API_PORT=4001 \
    GRPC_PORT=50051 \
    DATA_DIR=/var/lib/kafkaesque/data \
    OFFSETS_DIR=/var/lib/kafkaesque/offsets \
    RETENTION_HOURS=168 \
    LOG_LEVEL=info

# Copy release from build stage
COPY --from=build --chown=app:app /app/_build/prod/rel/kafkaesque ./

# Expose ports
EXPOSE 4000 4001 50051

# Health check
ENV HEALTH_PATH=/healthz
HEALTHCHECK --interval=10s --timeout=3s --retries=10 \
  CMD curl -fsS http://127.0.0.1:${PORT}${HEALTH_PATH} || exit 1

# Switch to app user
USER app

# Start the application
CMD ["/bin/bash", "-c", "mkdir -p ${DATA_DIR} ${OFFSETS_DIR} && exec ./bin/kafkaesque start"]