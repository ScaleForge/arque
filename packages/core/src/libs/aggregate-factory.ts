import { LRUCache } from 'lru-cache';
import { EventStore } from './event-store';
import { Aggregate, SnapshotOpts } from './aggregate';
import { Command, CommandHandler, Event, EventHandler } from './types';

export class AggregateFactory<
  TCommand extends Command = Command,
  TEvent extends Event = Event,
  TState = unknown,
> {
  private cache: LRUCache<
    string,
    Promise<Aggregate<TCommand, TEvent, TState>>
  >;

  constructor(
    private readonly eventStore: EventStore,
    private commandHandlers: CommandHandler<TCommand, TEvent, TState>[],
    private eventHandlers: EventHandler<TEvent, TState>[],
    private opts?: {
      readonly cacheOpts?: {
        max?: number;
        ttl?: number;
      };
      readonly snapshotOpts?: SnapshotOpts<TState>;
    }
  ) {
    this.cache = new LRUCache({
      max: opts?.cacheOpts?.max || 1000,
      ttl: opts?.cacheOpts?.ttl || 604800000, // 7 days
    });
  }

  public async load(
    id: Buffer,
    opts?: {
      noReload?: true,
      ignoreSnapshot?: true,
    }
  ): Promise<Aggregate<TCommand, TEvent, TState>> {
    const _id = id.toString('base64');

    let promise = this.cache.get(_id);

    if (!promise) {
      promise = (async () => {
        const aggregate = new Aggregate<TCommand, TEvent, TState>(
          this.eventStore,
          this.commandHandlers,
          this.eventHandlers,
          id,
          0,
          null as never,
          this.opts,
        );

        if (!opts?.noReload) {
          await aggregate.reload(opts);
        }

        return aggregate;
      })();

      this.cache.set(_id, promise);
    }

    return promise;
  }
}