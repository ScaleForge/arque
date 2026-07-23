/** eslint-disable @typescript-eslint/no-explicit-any */
import { LRUCache } from 'lru-cache';
import { Aggregate, AggregateOptions } from './aggregate';
import { StoreAdapter } from './adapters/store-adapter';
import { StreamAdapter } from './adapters/stream-adapter';

type ExtractState<T> = T extends Aggregate<infer State, any, any> ? State : never;
type ExtractCommandHandler<T> = T extends Aggregate<any, infer CommandHandler, any> ? CommandHandler : never;
type ExtractEventHandler<T> = T extends Aggregate<any, any, infer EventHandler> ? EventHandler : never;

type Options<T> = {
  readonly defaultState: ExtractState<T> | (() => ExtractState<T>);
  readonly cacheMax: number;
  readonly cacheTTL: number;
  readonly readPreference: 'primary' | 'secondary';
} & Partial<AggregateOptions<ExtractState<T>>>;

export class AggregateFactory<T extends Aggregate> {
  private readonly cache: LRUCache<
    string,
    Promise<T>
  >;

  private readonly opts: Options<T>;

  /**
   * @param store
   * @param stream
   * @param commandHandlers
   * @param eventHandlers
   * @param opts.defaultState - The default state of the aggregate. default: `null`
   * @param opts.cacheMax - The maximum number of aggregates to cache. default: `2046`
   * @param opts.cacheTTL - The time-to-live of the cache in milliseconds. default: `172800000` (48 hours)
   * @param opts.shouldTakeSnapshot - A function that determines if a snapshot should be taken. default: `undefined`
   * @param opts.snapshotInterval - The interval at which snapshots should be taken. default: `20`
   * @param opts.readPreference - The read preference for the aggregate. default: `secondary`
   */
  constructor(
    private readonly store: StoreAdapter,
    private readonly stream: StreamAdapter,
    private commandHandlers: ExtractCommandHandler<T>[],
    private eventHandlers: ExtractEventHandler<T>[],
    opts?: Partial<Options<T>>,
  ) {
    this.opts = {
      ...opts,
      defaultState: opts?.defaultState ?? null,
      shouldTakeSnapshot: opts?.shouldTakeSnapshot,
      cacheMax: opts?.cacheMax ?? 2_046,
      cacheTTL: opts?.cacheTTL ?? 14_400_000,
      snapshotInterval: opts?.snapshotInterval ?? 20,
      readPreference: opts?.readPreference ?? 'secondary',
    };

    this.cache = new LRUCache({
      max: this.opts.cacheMax,
      ttl: this.opts.cacheTTL,
    });
  }

  public async clear() {
    this.cache.clear();
  }

  public async load(
    id: Buffer,
    opts?: {
      noReload?: true,
    }
  ): Promise<T> {
    const _id = id.toString('base64');

    let promise = this.cache.get(_id);

    if (!promise) {
      promise = (async () => {
        const state = this.opts.defaultState instanceof Function ? this.opts.defaultState() : this.opts.defaultState;

        const aggregate = new Aggregate(
          this.store,
          this.stream,
          this.commandHandlers,
          this.eventHandlers,
          id,
          0,
          state,
          {
            shouldTakeSnapshot: this.opts.shouldTakeSnapshot,
            snapshotInterval: this.opts.snapshotInterval,
            serializeState: this.opts.serializeState,
            deserializeState: this.opts.deserializeState,
            readPreference: this.opts.readPreference,
          },
        ) as never as T;

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
