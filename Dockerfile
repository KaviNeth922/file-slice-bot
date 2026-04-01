FROM python:3.12-slim

# ── System packages ────────────────────────────────────────────────────────────
# nodejs 20+: required by bgutil POT server
# ffmpeg: video encode/decode
# supervisor: runs bot + POT server together
# git, curl: for cloning bgutil repo
RUN apt-get update && apt-get install -y \
    ffmpeg supervisor gcc python3-dev \
    nodejs npm git curl \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ────────────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── yt-dlp: always install latest (YouTube changes API constantly) ─────────────
RUN pip install --no-cache-dir -U yt-dlp

# ── bgutil POT HTTP server + yt-dlp plugin ─────────────────────────────────────
# This generates Proof-of-Origin tokens so YouTube doesn't block the
# datacenter IP with "Sign in to confirm you're not a bot".
# The HTTP server runs on port 4416 and yt-dlp plugin auto-fetches tokens.
RUN git clone --depth 1 https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /opt/bgutil

# Build the Node.js POT server
RUN cd /opt/bgutil/server && npm ci && npx tsc

# Install the yt-dlp plugin (auto-fetches POT from the local HTTP server)
RUN pip install --no-cache-dir -U bgutil-ytdlp-pot-provider

# ── App files ──────────────────────────────────────────────────────────────────
COPY bot.py .
COPY supervisord.conf .

RUN mkdir -p /tmp/tg_splitter /var/log/supervisor

CMD ["supervisord", "-c", "/app/supervisord.conf"]
