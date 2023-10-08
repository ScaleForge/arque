import { LRUCache } from 'lru-cache';
import { Aggregate, AggregateOpts } from './aggregate';
import { Command, CommandHandler, Event, EventHandler } from './types';
import { StoreAdapter } from './adapters/store-adapter';
import { StreamAdapter } from './adapters/stream-adapter';

export class AggregateFactory<
  TState = unknown,
  TCommandHandler extends CommandHandler<Command, Event, TState> = CommandHandler<Command, Event, TState>,
  TEventHandler extends EventHandler<Event, TState> = EventHandler<Event, TState>,
> {
  private cache: LRUCache<
    string,
    Promise<Aggregate<TState, TCommandHandler, TEventHandler>>
  >;

  constructor(
    private readonly store: StoreAdapter,
    private readonly stream: StreamAdapter,
    private commandHandlers: TCommandHandler[],
    private eventHandlers: TEventHandler[],
    private readonly opts: {
      readonly defaultState?: TState | (() => TState);
      readonly cacheMax?: number;
      readonly cacheTTL?: number;
    } & Partial<AggregateOpts<TState>>,
  ) {
    this.cache = new LRUCache({
      max: opts.cacheMax || 1000,
      ttl: opts.cacheTTL || 86400000, // 24 hours
    });
  }

  public async load(
    id: Buffer,
    opts?: {
      noReload?: true,
      reloadIfOlderThan?: number,
    }
  ): Promise<Aggregate<TState, TCommandHandler, TEventHandler>> {
    const _id = id.toString('base64');

    let promise = this.cache.get(_id);

    if (!promise) {
      promise = (async () => {
        const state = this.opts.defaultState ? 
          (this.opts.defaultState instanceof Function ? this.opts.defaultState() : this.opts.defaultState): null;

        const aggregate = new Aggregate<TState, TCommandHandler, TEventHandler>(
          this.store,
          this.stream,
          this.commandHandlers,
          this.eventHandlers,
          id,
          0,
          state as never,
          {
            shouldTakeSnapshot: this.opts.shouldTakeSnapshot,
            snapshotInterval: this.opts.snapshotInterval,
          },
        );

        if (!opts?.noReload) {
          await aggregate.reload();
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
      await aggregate.reload().catch(err => {
        this.cache.delete(_id);

        throw err;
      });
    }

    return aggregate;
  }
}