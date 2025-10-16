#
# Build the application in the `/app` directory using `uv`
#
FROM python:3.13 AS builder

# Get the latest `uv` and make it globally executable
COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /uvx /bin/

# Recommended by `uv` for prod builds, see docs for details
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

# Install dependencies
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Copy the app itself and install
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

#
# Create an image with the runnable cli tool, but without `uv`
#
FROM python:3.13-slim

# Install `nano` to support in-container file editing
RUN apt-get update && \
    apt-get install -y --no-install-recommends nano && \
    rm -rf /var/lib/apt/lists/*

# Copy the application from the builder
COPY --from=builder /app /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Use `/app` as the working directory
WORKDIR /app

# Add entrypoint
ENTRYPOINT ["pvduck"]
