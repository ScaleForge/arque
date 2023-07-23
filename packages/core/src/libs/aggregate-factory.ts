import { LRUCache } from 'lru-cache';
import { EventStore } from './event-store';
import { Aggregate, SnapshotOpts } from './aggregate';
import { CommandHandler, EventHandler } from './types';

export class AggregateFactory<
  TCommandHandler extends CommandHandler = CommandHandler,
  TEventHandler extends EventHandler = EventHandler,
  TState = unknown,
> {
  private cache: LRUCache<
    string,
    Promise<Aggregate<TCommandHandler, TEventHandler, TState>>
  >;

  constructor(
    private readonly eventStore: EventStore,
    private commandHandlers: TCommandHandler[],
    private eventHandlers: TEventHandler[],
    private readonly opts?: {
      readonly defaultState?: TState | (() => TState);
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
  ): Promise<Aggregate<TCommandHandler, TEventHandler, TState>> {
    const _id = id.toString('hex');

    let promise = this.cache.get(_id);

    if (!promise) {
      promise = (async () => {
        const state = this.opts?.defaultState ? 
          (this.opts.defaultState instanceof Function ? this.opts.defaultState() : this.opts.defaultState): null;

        const aggregate = new Aggregate<TCommandHandler, TEventHandler, TState>(
          this.eventStore,
          this.commandHandlers,
          this.eventHandlers,
          id,
          0,
          state as never,
          this.opts,
        );

        if (!opts?.noReload) {
          await aggregate.reload(opts);
        }

        return aggregate;
      })().catch(err => {
        this.cache.delete(_id);
        throw err;
      });

      this.cache.set(_id, promise);

      return promise;
    }

    const aggregate = await promise;

    if (!opts?.noReload) {
      await aggregate.reload(opts);
    }

    return aggregate;
  }
}