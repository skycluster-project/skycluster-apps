# Stage 1: Download and extract Fortio
FROM debian:bullseye-slim AS downloader

RUN apt-get update && apt-get install -y curl dpkg && rm -rf /var/lib/apt/lists/*

# Download Fortio .deb
RUN curl -L https://github.com/fortio/fortio/releases/download/v1.73.0/fortio_1.73.0_amd64.deb -o /tmp/fortio.deb

# Extract Fortio binary from .deb
RUN dpkg -x /tmp/fortio.deb /tmp/fortio && rm /tmp/fortio.deb

# Stage 2: Minimal final image
FROM debian:bullseye-slim

# Copy only the Fortio binary
COPY --from=downloader /tmp/fortio/usr/bin/fortio /usr/local/bin/fortio

# # Optional: set a small delay environment variable
# ENV START_DELAY=10

# # Entry point with delay
# ENTRYPOINT ["sh", "-c", "echo 'Delaying start for $START_DELAY seconds...'; sleep $START_DELAY; exec fortio \"$@\"", "--"]
