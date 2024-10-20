/* eslint-disable @typescript-eslint/no-explicit-any */
import { Mutex } from 'async-mutex';
import { backOff } from 'exponential-backoff';
import assert from 'assert';
import { debug } from 'debug';
import { Event, EventHandler, CommandHandler, Command } from './types';
import { EventId } from './event-id';
import { AggregateVersionConflictError, StoreAdapter } from './adapters/store-adapter';
import { StreamAdapter } from './adapters/stream-adapter';

type ExtractCommand<T> = T extends CommandHandler<infer Command, any, any> ? Command : never;

export type AggregateOptions<TState> = {
  readonly shouldTakeSnapshot?: (ctx: {
    aggregate: {
      id: Buffer;
      version: number;
    },
    state: TState;
  }) => boolean;
  readonly snapshotInterval?: number;
  readonly serializeState: (state: TState) => unknown;
  readonly deserializeState: (state: unknown) => TState;
};

export class Aggregate<
  TState = unknown,
  TCommandHandler extends CommandHandler<Command, Event, TState> = CommandHandler<Command, Event, TState>,
  TEventHandler extends EventHandler<Event, TState> = EventHandler<Event, TState>,
> {
  private readonly logger = {
    info: debug('info:Aggregate'),
    error: debug('error:Aggregate'),
    warn: debug('warn:Aggregate'),
    verbose: debug('verbose:Aggregate'),
    debug: debug('debug:Aggregate'),
  };

  private mutex: Mutex;

  private commandHandlers: Map<
    number,
    TCommandHandler
  >;

  private eventHandlers: Map<number, TEventHandler>;

  private opts: AggregateOptions<TState>;

  private _lastEvent: Event | null = null;

  constructor(
    private readonly store: StoreAdapter,
    private readonly stream: StreamAdapter,
    commandHandlers: TCommandHandler[],
    eventHandlers: TEventHandler[],
    private _id: Buffer,
    private _version: number,
    private _state: TState,
    opts?: Partial<AggregateOptions<TState>>,
  ) {
    this.mutex = new Mutex();

    this.commandHandlers = new Map(
      commandHandlers.map((item) => [item.type, item]),
    );

    this.eventHandlers = new Map(
      eventHandlers.map((item) => [item.type, item]),
    );

    this.opts = {
      ...opts,
      snapshotInterval: opts?.snapshotInterval ?? 100,
      serializeState: opts?.serializeState ?? (state => state),
      deserializeState: opts?.deserializeState ?? (state => state as TState),
    };
  }

  get id() {
    return this._id;
  }

  get version() {
    return this._version;
  }

  get state() {
    return this._state;
  }

  get lastEvent() {
    return this._lastEvent;
  }

  private commandHandler(type: number) {
    const handler = this.commandHandlers.get(type);

    assert(handler, `command handler does not exist: type=${type}`);

    return handler;
  }

  private eventHandler(type: number) {
    const handler = this.eventHandlers.get(type);

    assert(handler, `event handler does not exist: type=${type}`);

    return handler;
  }

  private shoudTakeSnapshot() {
    const { shouldTakeSnapshot, snapshotInterval } = this.opts;

    if (shouldTakeSnapshot) {
      return shouldTakeSnapshot({
        aggregate: {
          id: this.id,
          version: this.version,
        },
        state: this.state,
      });
    }

    return this.version % snapshotInterval === 0;
  }

  private async digest(
    events: AsyncIterable<Event> | Array<Event>,
  ) {
    for await (const event of events) {
      const state = await this.eventHandler(event.type).handle(
        {
          aggregate: {
            id: this.id,
            version: this.version,
          },
          state: this.state,
        },
        event,
      );

      this._state = state as TState;
      this._version = event.aggregate.version;
      this._lastEvent = event;
    }
  }

  private async _reload() {
    const snapshot = await this.store.findLatestSnapshot<TState>({
      aggregate: {
        id: this.id,
        version: this.version,
      },
    });

    if (snapshot) {
      this._state = this.opts.deserializeState(snapshot.state);
      this._version = snapshot.aggregate.version;
    }

    const events = await this.store.listEvents({
      aggregate: {
        id: this.id,
        version: this.version,
      },
    });

    await this.digest(events);
  }

  public async reload() {
    const release = await this.mutex.acquire();

    try {
      await this._reload();
    } finally {
      release();
    }
  }

  private async dispatch(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta' | 'timestamp'>[];
  }, ctx?: Buffer) {
    await this.store.saveEvents(params);

    const events = params.events.map((item, index) => ({
      ...item,
      timestamp: item.timestamp,
      aggregate: {
        id: this.id,
        version: this.version + index + 1,
      },
      meta: {
        ...item.meta,
        __ctx: ctx,
      },
    }));

    await this.stream.sendEvents([
      {
        stream: 'main',
        events,
      },
    ]);

    await this.digest(events);

    if (this.shoudTakeSnapshot()) {
      this.store.saveSnapshot({
        aggregate: {
          id: this.id,
          version: this.version,
        },
        state: this.opts.serializeState(this.state),
        timestamp: params.timestamp,
      });
    }
  }

  public async process(command: ExtractCommand<TCommandHandler>, ctx?: Buffer, opts?: {
    noReload?: true,
    maxRetries?: number,
  }): Promise<void> {
    const handler = this.commandHandler(command.type);

    const release = await this.mutex.acquire();

    let first = true;

    if (opts?.noReload !== true) {
      await this._reload();
    }

    try {
      await backOff(async () => {
        if (!first) {
          await this._reload();
        }

        first = false;

        const timestamp = new Date();

        const event = await handler.handle(
          {
            aggregate: {
              id: this.id,
              version: this.version,
            },
            state: this.state,
          },
          command,
          ...command.args,
        );

        await this.dispatch({
          aggregate: {
            id: this.id,
            version: this.version + 1,
          },
          events: (event instanceof Array ? event : [event]).map(item => ({
            id: EventId.generate(),
            type: item.type,
            body: item.body,
            meta: item.meta ?? {},
            timestamp: item.timestamp ?? timestamp,
          })),
          timestamp,
        }, ctx);
      }, {
        delayFirstAttempt: false,
        jitter: 'full',
        maxDelay: 400,
        numOfAttempts: opts?.maxRetries ?? 10,
        startingDelay: 10,
        timeMultiple: 2,
        retry: (err) => {
          if (err instanceof AggregateVersionConflictError) {
            this.logger.warn(`retrying: error="${err.message}"`);

            return true;
          }
          
          return false;
        },
      });
    } finally {
      release();
    }
  }
}
