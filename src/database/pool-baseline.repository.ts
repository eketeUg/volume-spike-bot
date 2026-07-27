import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import {
  PoolBaseline,
  PoolBaselineDocument,
} from './schemas/pool-baseline.schema';

@Injectable()
export class PoolBaselineRepository {
  private readonly logger = new Logger(PoolBaselineRepository.name);

  constructor(
    @InjectModel(PoolBaseline.name)
    private readonly model: Model<PoolBaselineDocument>,
  ) {}

  /**
   * Upsert a midnight baseline for a pool.
   * Safe to call multiple times — won't create duplicates.
   */
  async upsertBaseline(
    address: string,
    network: string,
    name: string,
    baselineH24: number,
    baselineOhlcvVolume = 0,
    targetDateStr?: string,
  ): Promise<void> {
    const today = targetDateStr || this.dateStr();
    const normalizedAddr = address.toLowerCase();
    const normalizedNet = network.toLowerCase();

    await this.model.findOneAndUpdate(
      { address: normalizedAddr, network: normalizedNet },
      {
        $set: {
          address: normalizedAddr,
          network: normalizedNet,
          name,
          baselineH24,
          baselineOhlcvVolume,
          baselineDate: today,
          capturedAt: new Date(),
          alertedAt: null,
        },
      },
      { upsert: true, returnDocument: 'after' },
    );
  }

  /**
   * Upsert a trending pool harvested from the 10-minute cron.
   * Uses $setOnInsert for alertedAt to preserve existing 6-hour cooldowns!
   */
  async upsertTrendingPool(
    address: string,
    network: string,
    name: string,
    h24: number,
    poolId = '',
    pairId = '',
    baselineOhlcvVolume = 0,
  ): Promise<void> {
    const today = this.dateStr();
    const normalizedAddr = address.toLowerCase();
    const normalizedNet = network.toLowerCase();

    await this.model.findOneAndUpdate(
      { address: normalizedAddr, network: normalizedNet },
      {
        $set: {
          name,
          baselineH24: h24,
          baselineDate: today,
          poolId,
          pairId,
          capturedAt: new Date(),
        },
        $setOnInsert: {
          address: normalizedAddr,
          network: normalizedNet,
          baselineOhlcvVolume,
          alertedAt: null,
        },
      },
      { upsert: true, returnDocument: 'after' },
    );
  }

  /**
   * Find today's baseline for a given pool.
   * Returns null if no snapshot exists for today yet.
   */
  async findBaseline(
    address: string,
    network: string,
  ): Promise<PoolBaselineDocument | null> {
    const today = this.dateStr();
    const normalizedAddr = address.toLowerCase();
    const normalizedNet = network.toLowerCase();
    return this.model.findOne({
      address: normalizedAddr,
      network: normalizedNet,
      baselineDate: today,
    });
  }

  /**
   * Find all baseline documents across all pools in MongoDB.
   */
  async findAllMonitoredPools(): Promise<PoolBaselineDocument[]> {
    return this.model.find({});
  }

  /**
   * Record that an alert was just sent for this pool.
   * Used to enforce the 6-hour cooldown.
   */
  async setAlertedAt(address: string, network: string): Promise<void> {
    const normalizedAddr = address.toLowerCase();
    const normalizedNet = network.toLowerCase();

    await this.model.updateOne(
      { address: normalizedAddr, network: normalizedNet },
      { $set: { alertedAt: new Date() } },
    );
  }

  /**
   * Delete ALL baseline documents.
   * Called by the 23:59 reset cron to clear the slate before midnight.
   */
  async clearAllBaselines(): Promise<void> {
    const result = await this.model.deleteMany({});
    this.logger.log(
      `🧹 Reset cron: cleared ${result.deletedCount} baseline documents`,
    );
  }

  /** YYYY-MM-DD in local time */
  private dateStr(): string {
    const d = new Date();
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }
}
