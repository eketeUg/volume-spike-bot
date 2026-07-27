# 🚨 Volume Spike Bot

High-performance DEX liquidity pool volume monitor built with **NestJS**, **MongoDB**, **GeckoTerminal `p1` API** (300 req/min limit), and **DexScreener API**.

---

## 🌟 Key Features

* **Dual-Process Architecture**:
  1. **🌾 Harvester Process**: Periodically scans top trending pools across networks sorted by 6-hour trend score (`-6h_trend_score`) and stores them in MongoDB.
  2. **⚙️ Volume Worker Process**: Continuously iterates through stored pools using 1D daily candles to calculate volume spikes in real time (200ms rate-limit gap).
* **3 Configurable Spike Modes (`Option A`)**:
  - 📅 **`SPIKE_MODE=daily`**: Compares Today's 1D candle volume vs Yesterday's 1D candle volume.
  - 📊 **`SPIKE_MODE=weekly`**: Compares Current Calendar Week (Mon--Today) vs Last Completed Calendar Week (Mon--Sun).
  - 📈 **`SPIKE_MODE=monthly`**: Compares Current Calendar Month (1st--Today) vs Last Completed Calendar Month (1st--End).
* **Multi-Chain Support**: Ethereum (`eth`), Binance Smart Chain (`bsc`), Robinhood (`robinhood`), and Solana (`solana`).
* **Smart Alerting Gates**:
  - Minimum volume threshold gate (`MIN_VOLUME_USD`).
  - Minimum spike multiplier gate (`MIN_SPIKE_MULTIPLIER`).
  - Real-time liquidity validation gate via DexScreener (`MIN_RESERVE_USD`).
  - **Once-Per-Day Alert Rule**: Prevents Telegram alert spamming for the same token until tomorrow.
* **Daily Midnight Reset (23:58 UTC)**: Wipes daily baselines and cooldown records so all tokens start fresh at 00:00 UTC.

---

## 📁 Environment Configuration

All settings and API endpoints are externalized and fully configurable via environment files:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PORT` | HTTP Server Port | `3000` |
| `SPIKE_MODE` | Calculation mode (`daily`, `weekly`, `monthly`) | `daily` |
| `NETWORKS` | Comma-separated networks to monitor | `eth,bsc,robinhood` |
| `HARVEST_INTERVAL_MINUTES` | Minutes between trending pool scans | `10` |
| `CYCLE_COOLDOWN_MINUTES` | Worker pause minutes after completing a pool cycle | `3` |
| `MIN_VOLUME_USD` | Minimum volume in USD required to trigger alert | `500000` |
| `MIN_SPIKE_MULTIPLIER` | Minimum volume spike multiplier (e.g. `5` for 5x) | `5` |
| `MIN_RESERVE_USD` | Minimum liquidity in USD required | `300000` |
| `MONGODB_URI` | MongoDB Atlas / Local connection URL | Required |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API Token | Required |
| `TELEGRAM_CHAT_ID` | Telegram Channel / Chat ID | Required |
| `GECKOTERMINAL_P1_BASE_URL` | GeckoTerminal `p1` API base URL | `https://app.geckoterminal.com/api/p1` |
| `DEXSCREENER_API_BASE_URL` | DexScreener API base URL | `https://api.dexscreener.com/latest/dex/pairs` |
| `DEXSCREENER_WEB_BASE_URL` | DexScreener web URL | `https://dexscreener.com` |

---

## 🚀 Running 3 Independent Instances (`Option A`)

To run Daily, Weekly, and Monthly bots simultaneously, create 3 environment files:

### 1. `.env.daily` (Daily Bot - Port 3000)
```env
PORT=3000
SPIKE_MODE=daily
NETWORKS=eth,bsc,robinhood
MIN_VOLUME_USD=500000
MIN_SPIKE_MULTIPLIER=5
MONGODB_URI=your_mongodb_atlas_daily_uri
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_daily_channel_id
```

### 2. `.env.weekly` (Weekly Bot - Port 3001)
```env
PORT=3001
SPIKE_MODE=weekly
NETWORKS=eth,bsc,robinhood
MIN_VOLUME_USD=1000000
MIN_SPIKE_MULTIPLIER=3
MONGODB_URI=your_mongodb_atlas_weekly_uri
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_weekly_channel_id
```

### 3. `.env.monthly` (Monthly Bot - Port 3002)
```env
PORT=3002
SPIKE_MODE=monthly
NETWORKS=eth,bsc,robinhood
MIN_VOLUME_USD=3000000
MIN_SPIKE_MULTIPLIER=2
MONGODB_URI=your_mongodb_atlas_monthly_uri
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_monthly_channel_id
```

---

## 🐳 Docker Deployment (VPS)

Deploy all 3 bots using **Docker Compose** on any VPS (Ubuntu/Debian/CentOS):

### Step 1: Clone Repository & Create `.env` files
```bash
git clone https://github.com/your-username/volume-spike-bot.git
cd volume-spike-bot

# Create .env.daily, .env.weekly, .env.monthly with your credentials
```

### Step 2: Launch All Containers
```bash
docker compose up -d --build
```

### Step 3: View Container Status & Live Logs
```bash
# View status of all running bots
docker compose ps

# View live logs for all 3 bots combined
docker compose logs -f

# View live logs for a specific bot instance
docker compose logs -f bot-daily
docker compose logs -f bot-weekly
docker compose logs -f bot-monthly
```

---

## 🛠️ Local Development

```bash
# Install dependencies
npm install

# Build application
npm run build

# Start local dev server
npm run start:dev
```

---

## 📜 License

MIT License.
