import { ConfigAdapter, StreamAdapter, Subscriber } from './adapters';
import debug from 'debug';

export class Broker {
  private readonly logger = {
    info: debug('info:Broker'),
    error: debug('error:Broker'),
    warn: debug('warn:Broker'),
    verbose: debug('verbose:Broker'),
    debug: debug('debug:Broker'),
  };

  private subscriber: Subscriber | null = null;

  constructor(
    private readonly config: ConfigAdapter,
    private readonly stream: StreamAdapter,
  ) {}

  async start(): Promise<void> {
    this.subscriber = await this.stream.subscribe('main', async event => {
      const streams = await this.config.findStreams(event.type);

      if (streams.length === 0) {
        this.logger.warn(`no streams found for event type: ${event.type}`);
        
        return;
      }

      await this.stream.sendEvents(streams.map(stream => ({
        stream,
        events: [event],
      })), { raw: true });
    }, { raw: true });
  }

  async stop(): Promise<void> {
    if (this.subscriber) {
      await this.subscriber.stop();
    }
  }
}