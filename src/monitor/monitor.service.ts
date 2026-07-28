import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { TelegramService } from '../telegram/telegram.service';
import { PoolBaselineRepository } from '../database/pool-baseline.repository';
import { PoolBaselineDocument } from '../database/schemas/pool-baseline.schema';
import axios from 'axios';
import { HttpsProxyAgent } from 'https-proxy-agent';

// ─── Types ────────────────────────────────────────────────────────────────────

interface GeckoPool {
  id: string;
  attributes: {
    address?: string;
    name?: string;
    volume_usd?: { h1?: string; h6?: string; h24?: string };
    base_token_price_usd?: string;
    reserve_in_usd?: string;
  };
  relationships?: {
    base_token?: { data?: { id?: string } };
    dex?: { data?: { id?: string } };
  };
}

// ─── Service ──────────────────────────────────────────────────────────────────

@Injectable()
export class VolumeMonitorService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(VolumeMonitorService.name);
  private intervalId?: NodeJS.Timeout;
  private isSnapshotting = false;
  private isPolling = false;

  // Config
  private pollingIntervalMinutes: number;
  private harvestIntervalMinutes: number;
  private cycleCooldownMinutes: number;
  private workerBatchSize: number;
  private minVolumeUsd: number;
  private minSpikeMultiplier: number;
  private minReserveUsd: number;
  private harvestTimerId: any;
  private spikeMode: 'daily' | 'weekly' | 'monthly' = 'daily';

  // External API & Web Base URLs (configurable from .env)
  private geckoTerminalV2BaseUrl: string;
  private geckoTerminalP1BaseUrl: string;
  private dexScreenerApiBaseUrl: string;
  private dexScreenerWebBaseUrl: string;

  // Track single Cloudflare block admin alert flag
  private hasNotifiedCloudflareBlock = false;

  // HTTP Proxy Agents for Cloudflare/402 bypass on VPS
  private proxyAgents: HttpsProxyAgent<string>[] = [];
  private proxyIndex = 0;

  // Rate-limit guard — GeckoTerminal free tier: 30 req/min
  private lastRequestAt = 0;
  private readonly MIN_REQUEST_GAP_MS = 2100;

  // Rate-limit guard — DexScreener free tier: 300 req/min
  private lastDexScreenerRequestAt = 0;
  private readonly DEXSCREENER_GAP_MS = 250;

  // Networks to monitor (configurable from .env NETWORKS=eth,bsc,solana,robinhood)
  private networks: string[] = ['eth', 'bsc', 'robinhood'];

  // Tokens to ignore as the *base* token (native coins & stablecoins)
  private readonly BLOCKED_SYMBOLS = new Set([
    // Stablecoins
    'USDT',
    'USDC',
    'DAI',
    'BUSD',
    'USDS',
    'TUSD',
    'FRAX',
    'USDD',
    'FDUSD',
    'PYUSD',
    'EURC',
    'LUSD',
    'USDP',
    'GUSD',
    'CRVUSD',
    'SUSD',
    'USDE',
    // Native & wrapped
    'ETH',
    'WETH',
    'BNB',
    'WBNB',
    'SOL',
    'WSOL',
    'BTC',
    'WBTC',
    'MATIC',
    'WMATIC',
    'AVAX',
    'WAVAX',
    'FTM',
    'WFTM',
  ]);

  constructor(
    private readonly configService: ConfigService,
    private readonly telegramService: TelegramService,
    private readonly baselineRepo: PoolBaselineRepository,
  ) {}

  // ─── Lifecycle ──────────────────────────────────────────────────────────────

  onModuleInit() {
    const rawProxy = this.configService.get<string>('HTTP_PROXY');
    if (rawProxy) {
      const proxyList = rawProxy
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      this.proxyAgents = proxyList.map((url) => new HttpsProxyAgent(url));
      this.logger.log(
        `🌐 [Proxy Engine Active] Configured ${this.proxyAgents.length} HTTP Proxy URL(s) for Cloudflare/402 bypass.`,
      );
    } else {
      this.logger.warn(
        `⚠️ [Proxy Engine Inactive] No HTTP_PROXY found in .env — using raw VPS IP.`,
      );
    }

    this.geckoTerminalV2BaseUrl = this.configService.get<string>(
      'GECKOTERMINAL_V2_BASE_URL',
      'https://api.geckoterminal.com/api/v2',
    );
    this.geckoTerminalP1BaseUrl = this.configService.get<string>(
      'GECKOTERMINAL_P1_BASE_URL',
      'https://app.geckoterminal.com/api/p1',
    );
    this.dexScreenerApiBaseUrl = this.configService.get<string>(
      'DEXSCREENER_API_BASE_URL',
      'https://api.dexscreener.com/latest/dex/pairs',
    );
    this.dexScreenerWebBaseUrl = this.configService.get<string>(
      'DEXSCREENER_WEB_BASE_URL',
      'https://dexscreener.com',
    );

    const rawNetworks = this.configService.get<string>(
      'NETWORKS',
      'eth,bsc,robinhood',
    );
    this.networks = rawNetworks
      .split(',')
      .map((s) => s.trim().toLowerCase())
      .filter((s) => s.length > 0);

    const rawMode = this.configService
      .get<string>('SPIKE_MODE', 'daily')
      .toLowerCase();
    this.spikeMode =
      rawMode === 'weekly' || rawMode === 'monthly' ? rawMode : 'daily';

    this.pollingIntervalMinutes = Number(
      this.configService.get<number>('POLLING_INTERVAL_MINUTES', 10),
    );
    this.harvestIntervalMinutes = Number(
      this.configService.get<number>(
        'HARVEST_INTERVAL_MINUTES',
        this.pollingIntervalMinutes,
      ),
    );
    this.cycleCooldownMinutes = Number(
      this.configService.get<number>('CYCLE_COOLDOWN_MINUTES', 3),
    );
    this.workerBatchSize = Number(
      this.configService.get<number>('WORKER_BATCH_SIZE', 1),
    );
    this.minVolumeUsd = Number(
      this.configService.get<number>('MIN_VOLUME_USD', 500_000),
    );
    this.minSpikeMultiplier = Number(
      this.configService.get<number>('MIN_SPIKE_MULTIPLIER', 5),
    );
    this.minReserveUsd = Number(
      this.configService.get<number>('MIN_RESERVE_USD', 300_000),
    );

    this.logger.log('🚀 Volume Monitor initialized');
    this.logger.log(
      `Settings: Mode=${this.spikeMode.toUpperCase()} | ` +
        `Networks=[${this.networks.join(', ')}] | ` +
        `HarvestInterval=${this.harvestIntervalMinutes}m | ` +
        `CycleCooldown=${this.cycleCooldownMinutes}m | ` +
        `WorkerBatchSize=${this.workerBatchSize} | ` +
        `MinVol=$${this.minVolumeUsd.toLocaleString()} | ` +
        `MinSpike=${this.minSpikeMultiplier}× | ` +
        `MinLiquidity=$${this.minReserveUsd.toLocaleString()}`,
    );

    // Initial harvest on startup
    this.harvestTrendingPools().catch((e) =>
      this.logger.error(`Initial harvest error: ${e.message}`),
    );

    // Dynamic Harvester interval timer from .env
    const harvestMs = this.harvestIntervalMinutes * 60 * 1000;
    this.harvestTimerId = setInterval(() => {
      this.harvestTrendingPools().catch((e) =>
        this.logger.error(`Harvester error: ${e.message}`),
      );
    }, harvestMs);

    // Start continuous background cycled worker loop
    this.startCycledWorker().catch((e) =>
      this.logger.error(`Cycled worker error: ${e.message}`),
    );
  }

  onModuleDestroy() {
    this.isWorkerRunning = false;
    if (this.harvestTimerId) clearInterval(this.harvestTimerId);
  }

  // ─── Cron: 23:58 UTC — Daily Midnight Reset ───────────────────────────────

  /**
   * Fires at 23:58 UTC every day.
   * Clears all old pool baselines from MongoDB so the new trading day starts with a fresh slate.
   */
  @Cron('58 23 * * *', { name: 'nightly-reset' })
  async resetDailyBaselines(): Promise<void> {
    this.logger.log(
      '🧹 [Midnight Reset] Clearing all pool baselines for the new UTC trading day...',
    );
    await this.baselineRepo.clearAllBaselines();
    this.hasNotifiedCloudflareBlock = false;
    this.logger.log(
      '✅ [Midnight Reset] Database cleared. Harvester will populate fresh pools for today.',
    );
  }

  /**
   * Send a single Admin Telegram alert when Cloudflare WAF (403 Forbidden) blocks the VPS IP
   */
  private async notifyCloudflareBlock(url: string): Promise<void> {
    if (this.hasNotifiedCloudflareBlock) return;
    this.hasNotifiedCloudflareBlock = true;

    const adminChatId =
      this.configService.get<string>('TELEGRAM_ADMIN_CHAT_ID') ||
      this.configService.get<string>('TELEGRAM_CHAT_ID') ||
      '';

    if (!adminChatId) {
      this.logger.error(
        'No TELEGRAM_ADMIN_CHAT_ID or TELEGRAM_CHAT_ID configured for Cloudflare 403 alert.',
      );
      return;
    }

    const msg =
      `⚠️ <b>[CLOUDFLARE BLOCK WARNING]</b> ⚠️\n\n` +
      `<b>Mode:</b> ${this.spikeMode.toUpperCase()}\n` +
      `<b>Status:</b> 403 Forbidden (Cloudflare WAF Blocked VPS IP)\n` +
      `<b>Endpoint:</b> <code>${url}</code>\n\n` +
      `<i>Note: This alert is sent ONLY ONCE per day to prevent Telegram spamming. Please inspect VPS IP reputation or proxy settings.</i>`;

    this.logger.error(
      `🚨 Cloudflare WAF 403 Block detected! Sending Admin alert to Telegram Chat ID: ${adminChatId}`,
    );
    await this.telegramService.sendMessage(adminChatId, msg);
  }

  // ─── Process 1: Trending Pool Harvester Cron (p1 API) ────────────────────

  /**
   * Runs every 10 minutes.
   * Scans live top trending pools from app.geckoterminal.com p1 API (-6h_trend_score) with 300 req/min limit.
   * Upserts new and existing trending pools into MongoDB without touching alertedAt timestamps.
   */
  async harvestTrendingPools(): Promise<void> {
    this.logger.log(
      '🌾 [Harvester] Scanning live top trending pools from api.geckoterminal.com v2 API...',
    );
    let totalHarvested = 0;

    for (const network of this.networks) {
      for (let page = 1; page <= 10; page++) {
        const url = `${this.geckoTerminalV2BaseUrl}/networks/${network}/trending_pools?page=${page}`;

        const response = await this.fetchV2(url);
        if (!response) continue;

        const pools: any[] = response.data?.data ?? [];
        if (pools.length === 0) break;

        for (const pool of pools) {
          const attr = pool.attributes ?? {};
          const address: string = attr.address ?? attr.api_address ?? '';
          const name: string = attr.name ?? 'Unknown';
          const h24 = parseFloat(
            attr.volume_usd?.h24 ??
              attr.from_volume_in_usd ??
              attr.to_volume_in_usd ??
              '0',
          );
          const poolId: string = pool.id ?? '';
          const pairId: string =
            pool.relationships?.pairs?.data?.[0]?.id ??
            pool.relationships?.pair?.data?.id ??
            '';

          if (!address || h24 <= 0) continue;
          if (this.isBlockedPool(name)) continue;

          await this.baselineRepo.upsertTrendingPool(
            address,
            network,
            name,
            h24,
            poolId,
            pairId,
          );
          totalHarvested++;
        }
      }
    }

    this.logger.log(
      `✅ [Harvester] Successfully harvested ${totalHarvested} trending pools into MongoDB.`,
    );
  }

  // ─── Process 2: Continuous Cycled OHLCV Volume Worker Loop ────────────────

  private isWorkerRunning = false;

  /**
   * Runs continuously in a non-blocking background loop:
   * 1. Fetches all monitored pools from MongoDB.
   * 2. Queries 1D candlesticks via p1 API (poolId/pairId) for exact daily volume.
   * 3. Validates liquidity ($300k+) via DexScreener and 6-hour cooldown before alerting.
   * 4. Cools down for 3 minutes at the end of each full cycle.
   */
  private async startCycledWorker(): Promise<void> {
    if (this.isWorkerRunning) return;
    this.isWorkerRunning = true;
    this.logger.log(
      '⚙️ Process 2: Continuous Cycled OHLCV Volume Worker started (p1 API, 300 req/min).',
    );

    while (this.isWorkerRunning) {
      try {
        const pools = await this.baselineRepo.findAllMonitoredPools();

        if (!pools || pools.length === 0) {
          this.logger.log(
            '⏳ No pools in MongoDB yet. Waiting 20s for Harvester...',
          );
          await this.delay(20_000);
          continue;
        }

        const cycleStartTime = Date.now();
        const totalPools = pools.length;
        const totalBatches = Math.ceil(totalPools / this.workerBatchSize);

        this.logger.log(
          `🔄 [Worker Cycle Started] Total Pools: ${totalPools} | ` +
            `Batch Size: ${this.workerBatchSize} | ` +
            `Est. Duration: ~${Math.ceil(totalPools * 0.25)}s`,
        );
        let checked = 0;
        let alerted = 0;
        let batchIndex = 0;

        const poolChunks = this.chunkArray(pools, this.workerBatchSize);

        for (const chunk of poolChunks) {
          if (!this.isWorkerRunning) break;
          batchIndex++;

          await Promise.all(
            chunk.map(async (doc) => {
              checked++;
              const {
                address,
                network,
                name,
                poolId,
                pairId,
                baselineOhlcvVolume,
                baselineH24,
              } = doc;
              if (this.isBlockedPool(name)) return;

              const ohlcvData = await this.fetchOhlcvV2(network, address);
              if (!ohlcvData) return;

              const todayVolume = ohlcvData.todayVolume;
              const yesterdayVolume = ohlcvData.yesterdayVolume;

              const effectiveBaseline = yesterdayVolume;
              // Filter out if rounded baseline volume is 0 (e.g. $0.29, $0.49, $0.00)
              if (Math.round(effectiveBaseline) <= 0 || todayVolume <= 0) return;

              const spike = todayVolume / effectiveBaseline;

              this.logger.debug(
                `[Worker OHLCV p1] ${name} (${network.toUpperCase()}) | ` +
                  `baseline=$${Math.round(effectiveBaseline).toLocaleString()} ` +
                  `today=$${Math.round(todayVolume).toLocaleString()} | spike=${spike.toFixed(2)}×`,
              );

              if (todayVolume < this.minVolumeUsd) return;

              if (spike >= this.minSpikeMultiplier) {
                const poolDetails = await this.fetchPoolDetails(
                  network,
                  address,
                );
                const reserveInUsd = poolDetails?.reserveInUsd ?? 0;
                const priceUsd = poolDetails?.priceUsd ?? null;
                const h1 = poolDetails?.h1 ?? 0;
                const rawDexId = poolDetails?.rawDexId ?? '';
                const tokenAddress = poolDetails?.tokenAddress ?? '';

                if (reserveInUsd < this.minReserveUsd) return;

                await this.triggerAlert(
                  network,
                  name,
                  address,
                  tokenAddress,
                  rawDexId,
                  priceUsd,
                  reserveInUsd,
                  h1,
                  todayVolume,
                  effectiveBaseline,
                  spike,
                );
                alerted++;
              }
            }),
          );

          const pendingPools = Math.max(0, totalPools - checked);
          const elapsedSecs = ((Date.now() - cycleStartTime) / 1000).toFixed(1);

          // Log progress every 100 pools or at the final batch
          if (checked % 100 === 0 || batchIndex === totalBatches) {
            this.logger.log(
              `⚙️ [Worker Progress] Checked: ${checked}/${totalPools} (${pendingPools} pending) | ` +
                `Elapsed: ${elapsedSecs}s | Alerts: ${alerted}`,
            );
          }

          // Safety gap between multi-item parallel batches only
          if (this.workerBatchSize > 1) {
            await this.delay(2000);
          }
        }

        const totalCycleTimeSecs = (
          (Date.now() - cycleStartTime) /
          1000
        ).toFixed(1);
        const coolDownMins = this.cycleCooldownMinutes;

        this.logger.log(
          `✅ [Worker Cycle Finished] Completed ${checked}/${totalPools} pools in ${totalCycleTimeSecs}s! ` +
            `(${alerted} alerts sent). Cooling down for ${coolDownMins}m before next cycle...`,
        );
        await this.delay(coolDownMins * 60 * 1000);
      } catch (err: any) {
        this.logger.error(`Error in Cycled OHLCV Worker loop: ${err.message}`);
        await this.delay(10_000);
      }
    }
  }

  // ─── Alert ────────────────────────────────────────────────────────────────

  private async triggerAlert(
    network: string,
    name: string,
    poolAddress: string,
    tokenAddress: string,
    rawDexId: string,
    priceUsd: number | null,
    reserveInUsd: number,
    h1: number,
    h24: number,
    baselineH24: number,
    spike: number,
  ): Promise<void> {
    // ── Once-per-day alert check via MongoDB ────────────────────────────────
    const dbDoc = await this.baselineRepo.findBaseline(poolAddress, network);

    if (dbDoc?.alertedAt) {
      this.logger.log(
        `⏳ ${name} (${poolAddress}) has already been alerted today — alert suppressed until tomorrow.`,
      );
      return;
    }

    await this.baselineRepo.setAlertedAt(poolAddress, network);

    const dexName = this.formatDexName(rawDexId, network);

    const dexChain = this.toDexScreenerChain(network);

    const chainName =
      network === 'eth'
        ? 'Ethereum'
        : network === 'bsc'
          ? 'BSC'
          : network === 'solana'
            ? 'Solana'
            : network === 'robinhood'
              ? 'Robinhood'
              : network.toUpperCase();

    const priceStr = priceUsd ? `$${priceUsd.toFixed(6)}` : 'N/A';

    const modeTitle =
      this.spikeMode === 'weekly'
        ? 'WEEKLY VOLUME SPIKE (7D)'
        : this.spikeMode === 'monthly'
          ? 'MONTHLY VOLUME SPIKE (30D)'
          : 'DAILY VOLUME SPIKE (1D)';

    const currentVolLabel =
      this.spikeMode === 'weekly'
        ? 'Last 7D Volume'
        : this.spikeMode === 'monthly'
          ? 'Last 30D Volume'
          : 'Current 24h Volume';

    const baselineVolLabel =
      this.spikeMode === 'weekly'
        ? 'Prev 7D Baseline'
        : this.spikeMode === 'monthly'
          ? 'Prev 30D Baseline'
          : 'Midnight Baseline';

    const msg =
      `🚨 <b>${modeTitle} [${chainName}]</b> 🚨\n\n` +
      `<b>Pool:</b> ${name}\n` +
      `🏦 <b>Exchange:</b> ${dexName}\n` +
      `🔗 <b>Pool Address</b> (tap to copy):\n<code>${poolAddress}</code>\n` +
      (tokenAddress
        ? `🪙 <b>Token Address</b> (tap to copy):\n<code>${tokenAddress}</code>\n`
        : '') +
      `\n🔥 <b>Spike:</b> <b>${spike.toFixed(1)}×</b> (${baselineVolLabel})\n` +
      `⏱ <b>Last 1h Vol:</b>       $${Math.round(h1).toLocaleString()}\n` +
      `📊 <b>${currentVolLabel}:</b>   $${Math.round(h24).toLocaleString()}\n` +
      `📅 <b>${baselineVolLabel}:</b> $${Math.round(baselineH24).toLocaleString()}\n` +
      `💰 <b>Price:</b>             ${priceStr}\n` +
      `🔒 <b>Liquidity:</b>         $${Math.round(reserveInUsd).toLocaleString()}\n\n` +
      `🔗 <a href="${this.dexScreenerWebBaseUrl}/${dexChain}/${poolAddress}">View on DexScreener</a>`;

    const chatId = process.env.TELEGRAM_CHAT_ID ?? '';
    const chatId_1 = process.env.TELEGRAM_CHAT_ID_1 ?? '';

    try {
      this.telegramService.sendMessage(chatId_1, msg);
      this.telegramService.sendMessage(chatId, msg);
    } catch (error) {
      this.logger.warn(`User yet to chat the bot`);
    }

    this.logger.log(
      `🚨 ALERT: ${name} (${chainName}) — ${spike.toFixed(1)}× | ` +
        `now=$${Math.round(h24).toLocaleString()} | baseline=$${Math.round(baselineH24).toLocaleString()}`,
    );
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /**
   * Returns true if the pool's base token is a native/stable coin,
   * or if both sides of the pair are blocked tokens.
   */
  private isBlockedPool(name: string): boolean {
    const [rawBase = '', rawQuote = ''] = name.split('/');
    const base = rawBase.trim().toUpperCase();
    const quote = rawQuote.trim().split(' ')[0].toUpperCase();
    if (this.BLOCKED_SYMBOLS.has(base)) return true;
    if (this.BLOCKED_SYMBOLS.has(base) && this.BLOCKED_SYMBOLS.has(quote))
      return true;
    return false;
  }

  /**
   * Convert a raw GeckoTerminal dex id into a readable exchange name.
   */
  private formatDexName(rawId: string, network: string): string {
    if (!rawId) return 'Unknown DEX';
    const suffixes = [
      `-${network}`,
      `-ethereum`,
      `-bsc`,
      `-solana`,
      `-robinhood`,
      `_${network}`,
      `_ethereum`,
      `_bsc`,
      `_solana`,
      `_robinhood`,
    ];
    let name = rawId;
    for (const s of suffixes) {
      if (name.endsWith(s)) {
        name = name.slice(0, -s.length);
        break;
      }
    }
    return name.replace(/[-_]/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  }

  /**
   * Fetch a URL from api.geckoterminal.com v2 API
   */
  private async fetchV2(url: string, maxRetries = 3): Promise<any | null> {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      const elapsed = Date.now() - this.lastRequestAt;
      if (elapsed < 100) {
        await this.delay(100 - elapsed);
      }
      this.lastRequestAt = Date.now();

      try {
        const reqConfig: any = {
          headers: {
            Accept: 'application/json',
            'User-Agent':
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
          },
          timeout: 10_000,
        };

        if (this.proxyAgents.length > 0) {
          const agent =
            this.proxyAgents[this.proxyIndex % this.proxyAgents.length];
          this.proxyIndex++;
          reqConfig.httpsAgent = agent;
          reqConfig.httpAgent = agent;
          reqConfig.proxy = false;
        }

        const response = await axios.get(url, reqConfig);
        return response;
      } catch (err: any) {
        if (err.response?.status === 429) {
          if (attempt >= maxRetries) return null;
          await this.delay(2000 * (attempt + 1));
          continue;
        }
        this.logger.warn(`v2 API HTTP error: ${err.message}`);
        return null;
      }
    }
    return null;
  }

  /**
   * Fetch 1D candlesticks from api.geckoterminal.com v2 API
   */
  private async fetchOhlcvV2(
    network: string,
    address: string,
  ): Promise<{ todayVolume: number; yesterdayVolume: number } | null> {
    try {
      let daysToFetch = 3;
      if (this.spikeMode === 'weekly') daysToFetch = 15;
      if (this.spikeMode === 'monthly') daysToFetch = 60;

      const candleUrl = `${this.geckoTerminalV2BaseUrl}/networks/${network}/pools/${address}/ohlcv/day?aggregate=1&limit=${daysToFetch}`;

      const resCandles = await this.fetchV2(candleUrl);
      const rawList: any[] =
        resCandles?.data?.data?.attributes?.ohlcv_list ?? [];

      if (!rawList || rawList.length === 0) return null;

      // Sort ascending by timestamp (oldest first)
      const candles = rawList
        .map((c) => ({
          ts: Number(c[0]),
          open: Number(c[1]),
          high: Number(c[2]),
          low: Number(c[3]),
          close: Number(c[4]),
          v: Number(c[5]) || 0,
        }))
        .sort((a, b) => a.ts - b.ts);

      if (candles.length === 0) return null;

      let todayVolume = 0;
      let yesterdayVolume = 0;

      if (this.spikeMode === 'weekly') {
        const todayDate = new Date();
        const currentDayOfWeek = todayDate.getUTCDay(); // 0 is Sun, 1 is Mon
        const daysSinceMonday =
          currentDayOfWeek === 0 ? 6 : currentDayOfWeek - 1;

        const currentWeekCandles = candles.slice(-(daysSinceMonday + 1));
        const prevWeekCandles = candles.slice(
          -(daysSinceMonday + 1 + 7),
          -(daysSinceMonday + 1),
        );

        todayVolume = currentWeekCandles.reduce((sum, c) => sum + c.v, 0);
        yesterdayVolume = prevWeekCandles.reduce((sum, c) => sum + c.v, 0);
      } else if (this.spikeMode === 'monthly') {
        const todayDate = new Date();
        const currentMonth = todayDate.getUTCMonth();
        const currentYear = todayDate.getUTCFullYear();

        let prevMonth = currentMonth - 1;
        let prevYear = currentYear;
        if (prevMonth < 0) {
          prevMonth = 11;
          prevYear -= 1;
        }

        const currentMonthCandles = candles.filter((c) => {
          const d = new Date(c.ts * 1000);
          return (
            d.getUTCMonth() === currentMonth &&
            d.getUTCFullYear() === currentYear
          );
        });

        const prevMonthCandles = candles.filter((c) => {
          const d = new Date(c.ts * 1000);
          return (
            d.getUTCMonth() === prevMonth && d.getUTCFullYear() === prevYear
          );
        });

        todayVolume = currentMonthCandles.reduce((sum, c) => sum + c.v, 0);
        yesterdayVolume = prevMonthCandles.reduce((sum, c) => sum + c.v, 0);
      } else {
        const todayCandle = candles[candles.length - 1];
        const yesterdayCandle =
          candles.length >= 2 ? candles[candles.length - 2] : todayCandle;
        todayVolume = todayCandle?.v ?? 0;
        yesterdayVolume = yesterdayCandle?.v ?? 0;
      }

      return { todayVolume, yesterdayVolume };
    } catch (err: any) {
      this.logger.warn(`v2 OHLCV fetch error for ${address}: ${err.message}`);
      return null;
    }
  }

  /**
   * Convert GeckoTerminal network slug to DexScreener chainId
   */
  private toDexScreenerChain(network: string): string {
    switch (network.toLowerCase()) {
      case 'eth':
        return 'ethereum';
      case 'bsc':
        return 'bsc';
      case 'solana':
        return 'solana';
      default:
        return network;
    }
  }

  /**
   * Split an array into chunks of a specified size
   */
  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const results: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      results.push(array.slice(i, i + chunkSize));
    }
    return results;
  }

  /**
   * Fetch live pool liquidity and price details from DexScreener
   */
  private async fetchPoolDetails(
    network: string,
    address: string,
  ): Promise<{
    reserveInUsd: number;
    priceUsd: number | null;
    h1: number;
    rawDexId: string;
    tokenAddress: string;
  } | null> {
    const dexChainId = this.toDexScreenerChain(network);
    const url = `${this.dexScreenerApiBaseUrl}/${dexChainId}/${address}`;
    const response = await this.fetchDexScreenerBatch(url);
    if (!response || !response.data?.pairs?.[0]) return null;

    const pair = response.data.pairs[0];
    return {
      reserveInUsd: parseFloat(pair.liquidity?.usd ?? '0'),
      priceUsd: parseFloat(pair.priceUsd ?? '0') || null,
      h1: parseFloat(pair.volume?.h1 ?? '0'),
      rawDexId: pair.dexId ?? '',
      tokenAddress: pair.baseToken?.address ?? '',
    };
  }

  /**
   * Fetch a DexScreener batch URL with rate-limit enforcement
   */
  private async fetchDexScreenerBatch(
    url: string,
    maxRetries = 3,
  ): Promise<any | null> {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      const elapsed = Date.now() - this.lastDexScreenerRequestAt;
      if (elapsed < 200) {
        await this.delay(200 - elapsed);
      }
      this.lastDexScreenerRequestAt = Date.now();

      try {
        const reqConfig: any = { timeout: 10_000 };
        if (this.proxyAgents.length > 0) {
          const agent =
            this.proxyAgents[this.proxyIndex % this.proxyAgents.length];
          this.proxyIndex++;
          reqConfig.httpsAgent = agent;
          reqConfig.httpAgent = agent;
          reqConfig.proxy = false;
        }

        const response = await axios.get(url, reqConfig);
        return response;
      } catch (err: any) {
        if (err.response?.status === 429) {
          if (attempt >= maxRetries) return null;
          await this.delay(2000 * (attempt + 1));
          continue;
        }
        this.logger.warn(`DexScreener HTTP error: ${err.message}`);
        return null;
      }
    }
    return null;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
