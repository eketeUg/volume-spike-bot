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
  private minVolumeUsd: number;
  private minSpikeMultiplier: number;
  private minReserveUsd: number;
  private harvestTimerId: any;
  private spikeMode: 'daily' | 'weekly' | 'monthly' = 'daily';

  // External API & Web Base URLs (configurable from .env)
  private geckoTerminalP1BaseUrl: string;
  private dexScreenerApiBaseUrl: string;
  private dexScreenerWebBaseUrl: string;

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
    this.logger.log(
      '✅ [Midnight Reset] Database cleared. Harvester will populate fresh pools for today.',
    );
  }

  // ─── Process 1: Trending Pool Harvester Cron (p1 API) ────────────────────

  /**
   * Runs every 10 minutes.
   * Scans live top trending pools from app.geckoterminal.com p1 API (-6h_trend_score) with 300 req/min limit.
   * Upserts new and existing trending pools into MongoDB without touching alertedAt timestamps.
   */
  @Cron('*/10 * * * *', { name: 'trending-harvester' })
  async harvestTrendingPools(): Promise<void> {
    this.logger.log(
      '🌾 [Harvester] Scanning live top trending pools from app.geckoterminal.com p1 API (-6h_trend_score)...',
    );
    let totalHarvested = 0;

    for (const network of this.networks) {
      for (let page = 1; page <= 10; page++) {
        const url = `${this.geckoTerminalP1BaseUrl}/${network}/pools?page=${page}&sort=-6h_trend_score`;

        const response = await this.fetchP1(url);
        if (!response) continue;

        const pools: any[] = response.data?.data ?? [];
        if (pools.length === 0) break;

        for (const pool of pools) {
          const attr = pool.attributes ?? {};
          const address: string = attr.address ?? attr.api_address ?? '';
          const name: string = attr.name ?? 'Unknown';
          const h24 = parseFloat(
            attr.from_volume_in_usd ?? attr.to_volume_in_usd ?? '0',
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

        this.logger.log(
          `🔄 [Worker Cycle] Starting p1 OHLCV volume checks across ${pools.length} pools in MongoDB...`,
        );
        let checked = 0;
        let alerted = 0;

        for (const doc of pools) {
          if (!this.isWorkerRunning) break;

          const {
            address,
            network,
            name,
            poolId,
            pairId,
            baselineOhlcvVolume,
            baselineH24,
          } = doc;
          if (this.isBlockedPool(name)) continue;

          // Fetch today's and yesterday's OHLCV daily candles using p1 API
          const ohlcvData = await this.fetchOhlcvP1(
            network,
            address,
            poolId,
            pairId,
          );
          if (!ohlcvData) continue;
          checked++;

          const todayVolume = ohlcvData.todayVolume;
          const yesterdayVolume = ohlcvData.yesterdayVolume;

          const effectiveBaseline =
            baselineOhlcvVolume > 0
              ? baselineOhlcvVolume
              : yesterdayVolume > 0
                ? yesterdayVolume
                : baselineH24;

          if (effectiveBaseline <= 0 || todayVolume <= 0) continue;

          const spike = todayVolume / effectiveBaseline;

          this.logger.debug(
            `[Worker OHLCV p1] ${name} (${network.toUpperCase()}) | ` +
              `baseline=$${Math.round(effectiveBaseline).toLocaleString()} ` +
              `today=$${Math.round(todayVolume).toLocaleString()} | spike=${spike.toFixed(2)}×`,
          );

          // Log progress every 100 pools
          if (checked % 100 === 0) {
            this.logger.log(
              `📊 Worker progress: checked ${checked}/${pools.length} pools...`,
            );
          }

          if (todayVolume < this.minVolumeUsd) continue;

          if (spike >= this.minSpikeMultiplier) {
            // Live liquidity and price check via DexScreener
            const poolDetails = await this.fetchPoolDetails(network, address);
            const reserveInUsd = poolDetails?.reserveInUsd ?? 0;
            const priceUsd = poolDetails?.priceUsd ?? null;
            const h1 = poolDetails?.h1 ?? 0;
            const rawDexId = poolDetails?.rawDexId ?? '';
            const tokenAddress = poolDetails?.tokenAddress ?? '';

            if (reserveInUsd < this.minReserveUsd) continue;

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
        }

        const coolDownMins = 3;
        this.logger.log(
          `✅ [Worker Cycle Complete] Checked ${checked}/${pools.length} pools (${alerted} alerts sent). ` +
            `Cooling down for ${coolDownMins} minutes before starting next cycle...`,
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
    this.telegramService.sendMessage(chatId, msg);
    // this.telegramService.sendMessage(chatId_1, msg);

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
   * Fetch a URL from app.geckoterminal.com p1 API with 300 req/min rate limit (200ms gap)
   */
  private async fetchP1(url: string, maxRetries = 3): Promise<any | null> {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      const elapsed = Date.now() - this.lastDexScreenerRequestAt;
      if (elapsed < 200) {
        await this.delay(200 - elapsed);
      }
      this.lastDexScreenerRequestAt = Date.now();

      try {
        const response = await axios.get(url, {
          headers: {
            'User-Agent':
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            Accept: 'application/json',
          },
          timeout: 10_000,
        });
        return response;
      } catch (err: any) {
        if (err.response?.status === 429) {
          if (attempt >= maxRetries) return null;
          await this.delay(2000 * (attempt + 1));
          continue;
        }
        this.logger.warn(`p1 API HTTP error: ${err.message}`);
        return null;
      }
    }
    return null;
  }

  /**
   * Fetch 1D candlesticks from app.geckoterminal.com p1 API
   */
  private async fetchOhlcvP1(
    network: string,
    address: string,
    poolId?: string,
    pairId?: string,
  ): Promise<{ todayVolume: number; yesterdayVolume: number } | null> {
    try {
      let activePoolId = poolId;
      let activePairId = pairId;

      if (!activePoolId || !activePairId) {
        const detailsUrl = `${this.geckoTerminalP1BaseUrl}/${network}/pools/${address}`;
        const res = await this.fetchP1(detailsUrl);
        if (!res?.data?.data) return null;

        activePoolId = res.data.data.id;
        activePairId =
          res.data.data.relationships?.pairs?.data?.[0]?.id ||
          res.data.data.relationships?.pair?.data?.id;
      }

      if (!activePoolId || !activePairId) return null;

      const now = Math.floor(Date.now() / 1000);
      let daysToFetch = 3;
      if (this.spikeMode === 'weekly') daysToFetch = 15;
      if (this.spikeMode === 'monthly') daysToFetch = 60;

      const fromTs = now - daysToFetch * 86400;
      const candleUrl = `${this.geckoTerminalP1BaseUrl}/candlesticks/${activePoolId}/${activePairId}?resolution=1D&from_timestamp=${fromTs}&to_timestamp=${now}`;

      const resCandles = await this.fetchP1(candleUrl);
      const candles: any[] = resCandles?.data?.data ?? [];
      if (!candles || candles.length === 0) return null;

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

        todayVolume = currentWeekCandles.reduce(
          (sum, c) => sum + (Number(c.v) || 0),
          0,
        );
        yesterdayVolume = prevWeekCandles.reduce(
          (sum, c) => sum + (Number(c.v) || 0),
          0,
        );
      } else if (this.spikeMode === 'monthly') {
        const todayDate = new Date();
        const currentMonth = todayDate.getUTCMonth();
        const currentYear = todayDate.getUTCFullYear();

        const currentMonthCandles = candles.filter((c) => {
          const d = new Date(c.dt);
          return (
            d.getUTCMonth() === currentMonth &&
            d.getUTCFullYear() === currentYear
          );
        });

        const prevMonthIndex = currentMonth === 0 ? 11 : currentMonth - 1;
        const prevMonthYear =
          currentMonth === 0 ? currentYear - 1 : currentYear;

        const prevMonthCandles = candles.filter((c) => {
          const d = new Date(c.dt);
          return (
            d.getUTCMonth() === prevMonthIndex &&
            d.getUTCFullYear() === prevMonthYear
          );
        });

        todayVolume = currentMonthCandles.reduce(
          (sum, c) => sum + (Number(c.v) || 0),
          0,
        );
        yesterdayVolume = prevMonthCandles.reduce(
          (sum, c) => sum + (Number(c.v) || 0),
          0,
        );
      } else {
        const todayCandle = candles[candles.length - 1];
        const yesterdayCandle =
          candles.length >= 2 ? candles[candles.length - 2] : todayCandle;
        todayVolume = Number(todayCandle?.v) || 0;
        yesterdayVolume = Number(yesterdayCandle?.v) || 0;
      }

      return { todayVolume, yesterdayVolume };
    } catch (err: any) {
      this.logger.warn(`p1 OHLCV fetch error for ${address}: ${err.message}`);
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
        const response = await axios.get(url, { timeout: 10_000 });
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
