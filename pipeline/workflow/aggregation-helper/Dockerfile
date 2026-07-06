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

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED=True

WORKDIR /app

# Copy dependency definition files to leverage Docker layer caching
COPY pyproject.toml uv.lock __init__.py ./

# Install production dependencies (without the project itself)
RUN uv sync --no-dev --no-install-project

# Copy local code to the container image
COPY . .

# Install the project itself
RUN uv sync --no-dev

# Place the virtual environment's bin directory on the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Run the Cloud Run Job script
ENTRYPOINT ["python", "main.py"]
