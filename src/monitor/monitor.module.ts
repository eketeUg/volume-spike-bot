import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import {
  PoolBaseline,
  PoolBaselineSchema,
} from '../database/schemas/pool-baseline.schema';
import { PoolBaselineRepository } from '../database/pool-baseline.repository';
import { VolumeMonitorService } from './monitor.service';
import { TelegramModule } from '../telegram/telegram.module';

@Module({
  imports: [
    TelegramModule,
    MongooseModule.forFeature([
      { name: PoolBaseline.name, schema: PoolBaselineSchema },
    ]),
  ],
  providers: [VolumeMonitorService, PoolBaselineRepository],
  exports: [VolumeMonitorService],
})
export class MonitorModule {}
