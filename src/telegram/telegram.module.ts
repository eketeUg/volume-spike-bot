import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TelegramService } from './telegram.service';

// We only SEND messages — no polling / getUpdates needed.
// So we skip nestjs-telegraf entirely and use the raw Telegraf
// HTTP client directly inside TelegramService.
@Module({
  imports: [ConfigModule],
  providers: [TelegramService],
  exports: [TelegramService],
})
export class TelegramModule {}
