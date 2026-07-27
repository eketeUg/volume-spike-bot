import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Telegraf } from 'telegraf';

@Injectable()
export class TelegramService {
  private readonly logger = new Logger(TelegramService.name);
  private readonly bot: Telegraf | null = null;

  constructor(private readonly configService: ConfigService) {
    const token = this.configService.get<string>('TELEGRAM_BOT_TOKEN');
    if (!token) {
      this.logger.error('TELEGRAM_BOT_TOKEN is not set — alerts will not be sent.');
      return;
    }
    // Instantiate Telegraf WITHOUT calling launch() or startPolling().
    // This gives us access to bot.telegram.sendMessage() over HTTPS
    // with zero long-polling / getUpdates calls — no 409 conflicts.
    this.bot = new Telegraf(token);
    this.logger.log('Telegram bot initialized (send-only mode, no polling).');
  }

  async sendMessage(chatId: number | string, text: string): Promise<void> {
    if (!this.bot) {
      this.logger.warn('Bot not initialized — cannot send message.');
      return;
    }
    try {
      await this.bot.telegram.sendMessage(chatId, text, {
        parse_mode: 'HTML',
        // Disable link previews so alerts stay compact
        link_preview_options: { is_disabled: true },
      });
    } catch (error: any) {
      this.logger.error(`Failed to send Telegram message: ${error.message}`);
    }
  }
}
