import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type PoolBaselineDocument = PoolBaseline & Document;

/**
 * Stores the midnight (00:00) h24 volume snapshot for each pool.
 *
 * TTL: `capturedAt` + 86400 seconds (24h) — MongoDB auto-deletes the
 * document at the same time the next day's cron fires, ensuring a clean slate.
 *
 * Compound unique index on { address, network } prevents duplicate snapshots
 * and makes upserts safe to re-run.
 */
@Schema({ collection: 'pool_baselines' })
export class PoolBaseline {
  /** Pool contract address */
  @Prop({ required: true })
  address: string;

  /** GeckoTerminal network slug: 'eth' | 'bsc' | 'solana' */
  @Prop({ required: true })
  network: string;

  /** GeckoTerminal internal pool ID */
  @Prop({ default: '' })
  poolId: string;

  /** GeckoTerminal internal pair ID */
  @Prop({ default: '' })
  pairId: string;

  /** Human-readable pool name e.g. "PEPE / WETH 0.05%" */
  @Prop()
  name: string;

  /** h24 volume at the time of snapshot (midnight) — used as baseline */
  @Prop({ required: true })
  baselineH24: number;

  /** 1D OHLCV daily candle volume captured at baseline */
  @Prop({ default: 0 })
  baselineOhlcvVolume: number;

  /** Calendar date of this snapshot e.g. "2026-07-22" */
  @Prop({ required: true })
  baselineDate: string;

  /**
   * Snapshot creation time.
   * Acts as the TTL field — document expires 24h after this timestamp.
   */
  @Prop({ required: true, expires: 86400 })
  capturedAt: Date;

  /** Last time an alert was sent for this pool (null = never) */
  @Prop({ type: Date, default: null })
  alertedAt: Date | null;
}

export const PoolBaselineSchema = SchemaFactory.createForClass(PoolBaseline);

// Unique compound index — prevents duplicate snapshots per pool per network
PoolBaselineSchema.index({ address: 1, network: 1 }, { unique: true });
