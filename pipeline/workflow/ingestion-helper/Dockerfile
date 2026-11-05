# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Pinned by digest for reproducible builds. Python 3.13 because the 3.12 image
# still bundles pip 25.0.1, which has known vulnerabilities.
FROM python:3.13-slim@sha256:09a17cad02668ae61ece136b4fd36ac3849e14f670ebedb637ea3e15580525bc

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /uvx /bin/

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED=True

# Use the image's Python instead of letting uv download one
ENV UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Pick up OS security fixes released after the base image was built
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Copy dependency definition files to leverage Docker layer caching
COPY pyproject.toml uv.lock __init__.py ./

# Install production dependencies (without the project itself)
RUN uv sync --frozen --no-dev --no-install-project

# Copy local code to the container image
COPY . .

# Install the project itself
RUN uv sync --frozen --no-dev

# Place the virtual environment's bin directory on the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Run the FastAPI service with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
