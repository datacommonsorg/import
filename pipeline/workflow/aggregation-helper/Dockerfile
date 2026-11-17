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

FROM python:3.12-slim

# Copy uv binary (pinned for reproducible builds)
COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /uvx /bin/

# Apply the latest Debian security patches to the base image
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# Remove the base image's system pip and the bundled ensurepip wheels.
# Dependencies are installed with uv, so pip is not needed at runtime.
RUN python -m pip uninstall -y pip && \
    rm -rf /usr/local/lib/python3.12/ensurepip/_bundled

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED=True

WORKDIR /app

# Copy dependency definition files to leverage Docker layer caching
COPY pyproject.toml uv.lock __init__.py ./

# Install production dependencies (without the project itself)
RUN uv sync --locked --no-dev --no-install-project

# Copy local code to the container image
COPY . .

# Install the project itself
RUN uv sync --locked --no-dev

# Place the virtual environment's bin directory on the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Run the Cloud Run Job script
ENTRYPOINT ["python", "main.py"]
