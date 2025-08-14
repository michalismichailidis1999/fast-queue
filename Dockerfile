# ---------- Stage 1: Build ----------
FROM ubuntu:22.04 AS build

# Install basic build tools
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    unzip \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh

# Copy vcpkg dependencies file
COPY vcpkg_rf.txt /app/vcpkg_rf.txt

# Install packages from vcpkg_rf.txt
RUN xargs -a /app/vcpkg_rf.txt -I {} /opt/vcpkg/vcpkg install {}

# Copy source code
COPY . .

# Configure and build with C++17 standard
RUN cmake -B build -S . \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    && cmake --build build -- -j$(nproc)

# ---------- Stage 2: Runtime ----------
FROM ubuntu:22.04 AS runtime

# Install runtime dependencies (if your binary needs shared libs from vcpkg)
RUN apt-get update && apt-get install -y \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built binary
COPY --from=build /app/build/FastQueue /app/FastQueue

ENTRYPOINT ["./FastQueue"]