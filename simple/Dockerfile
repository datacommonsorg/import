# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# #### Python runtime. ####
FROM python:3.11.18-slim AS runner

ARG ENV
ENV ENV=${ENV}

WORKDIR /workspace

# Apply security patches for OS base packages (OpenSSL, Perl, glibc, pcre2, etc.)
RUN apt-get update && apt-get upgrade -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy simple importer requirements.
COPY requirements.txt requirements_nodeps.txt ./import/simple/

ARG PIP_DISABLE_PIP_VERSION_CHECK=1
ARG PIP_NO_CACHE_DIR=1
ARG PIP_ROOT_USER_ACTION=ignore

RUN pip3 install -r ./import/simple/requirements.txt \
    && pip3 install --no-deps -r ./import/simple/requirements_nodeps.txt \
    && pip3 uninstall -y pip setuptools wheel \
    && rm -rf /usr/local/lib/python3.*/ensurepip/_bundled

# Copy only the packages the preprocessor runs (stats.main imports stats, util
# and kg_util). New directories under simple/ stay out of the image unless they
# are added here.
COPY stats/ ./import/simple/stats/
COPY util/ ./import/simple/util/
COPY kg_util/ ./import/simple/kg_util/
COPY pkg_resources.py ./import/simple/

# Copy executable script.
COPY run.sh .

# Make script executable.
RUN chmod +x run.sh

# TODO(b/583773160): Switch the runtime image to distroless, which doesn't ship
# these packages, and replace run.sh with a Python entrypoint (distroless has no
# shell).
# Remove unused Debian packages from the base image, they bring in vulnerabilities.
RUN dpkg --purge --force-all \
        apt libapt-pkg7.0 \
        bsdutils libblkid1 liblastlog2-2 libmount1 libsmartcols1 libuuid1 login mount util-linux \
        diffutils \
        libacl1 \
        libattr1 \
        libbz2-1.0 \
        libncursesw6 ncurses-base ncurses-bin \
        libpam-modules libpam-modules-bin libpam-runtime libpam0g \
        libsqlite3-0 \
        libsystemd0 libudev1 \
        login.defs passwd \
        tar \
    && dpkg --purge --force-all perl-base \
    && PYTHONPATH=/workspace/import/simple python3 -c "import stats.main, stats.runner"

# Set the entrypoint to force bash execution
ENTRYPOINT ["/bin/bash", "./run.sh"]
