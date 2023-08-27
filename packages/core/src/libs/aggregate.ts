import { Mutex } from 'async-mutex';
import { backOff } from 'exponential-backoff';
import assert from 'assert';
import { Event, EventHandler, CommandHandler, Command } from './types';
import { EventStore } from './event-store';
import { EventId } from './event-id';
import { AggregateVersionConflictError } from './error';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ExtractCommand<T> = T extends CommandHandler<infer Command, any, any> ? Command : never;

export type SnapshotOpts<TState = unknown> = {
  interval?: number;
  shouldTakeSnapshot?: (ctx: {
    aggregate: {
      id: Buffer;
      version: number;
    },
    state: TState;
  }) => boolean;
};

export class Aggregate<
  TState,
  TCommandHandler extends CommandHandler<Command, Event, TState> = CommandHandler<Command, Event, TState>,
  TEventHandler extends EventHandler<Event, TState> = EventHandler<Event, TState>,
> {
  private mutex: Mutex;

  private commandHandlers: Map<
    number,
    TCommandHandler
  >;

  private eventHandlers: Map<number, TEventHandler>;

  constructor(
    private readonly eventStore: EventStore,
    commandHandlers: TCommandHandler[],
    eventHandlers: TEventHandler[],
    private _id: Buffer,
    private _version: number,
    private _state: TState,
    private readonly opts?: {
      readonly snapshotOpts?: SnapshotOpts<TState>;
    },
  ) {
    this.mutex = new Mutex();

    this.commandHandlers = new Map(
      commandHandlers.map((item) => [item.type, item]),
    );

    this.eventHandlers = new Map(
      eventHandlers.map((item) => [item.type, item]),
    );
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

  private getCommandHandler(type: number) {
    const handler = this.commandHandlers.get(type);

    assert(handler, `command handler does not exist: type=${type}`);

    return handler;
  }

  private getEventHandler(type: number) {
    const handler = this.eventHandlers.get(type);

    assert(handler, `event handler does not exist: type=${type}`);

    return handler;
  }

  private shoudTakeSnapshot() {
    const { shouldTakeSnapshot, interval } = this.opts?.snapshotOpts || {};

    if (shouldTakeSnapshot) {
      return shouldTakeSnapshot({
        aggregate: {
          id: this.id,
          version: this.version,
        },
        state: this.state,
      });
    }

    return this.version % (interval || 100) === 0;
  }

  private async digest(
    events: AsyncIterable<Event> | Array<Event>,
    opts?: {
      disableSnapshot?: true
    },
  ) {
    for await (const event of events) {
      const eventHandler = this.getEventHandler(event.type);

      const state = await eventHandler.handle(
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
      
      if (!opts?.disableSnapshot && this.shoudTakeSnapshot()) {
        await this.eventStore.saveSnapshot({
          aggregate: {
            id: this.id,
            version: this.version,
          },
          state: this.state,
          timestamp: event.timestamp,
        });
      }
    }
  }

  private async _reload(
    opts?: {
      ignoreSnapshot?: true,
    },
  ) {
    if (!opts?.ignoreSnapshot) {
      const snapshot = await this.eventStore.getSnapshot<TState>({
        aggregate: {
          id: this.id,
          version: this.version,
        },
      });

      if (snapshot) {
        this._state = snapshot.state;
        this._version = snapshot.aggregate.version;
      }
    }

    await this.digest(await this.eventStore.listEvents({
      aggregate: {
        id: this.id,
        version: this.version,
      },
    }), {
      disableSnapshot: true,
    });
  }

  public async reload(opts?: {
    ignoreSnapshot?: true,
  }) {
    const release = await this.mutex.acquire();

    try {
      await this._reload(opts);
    } finally {
      release();
    }
  }

  public async process(command: ExtractCommand<TCommandHandler>, opts?: {
    noInitialReload?: true,
    ignoreSnapshot?: true,
    maxRetries?: number,
  }): Promise<void> {
    await backOff(async () => {
      const release = await this.mutex.acquire();

      try {
        if (!opts?.noInitialReload) {
          await this._reload(opts);
        }

        const commandHandler = this.getCommandHandler(command.type);

        const timestamp = new Date();

        const _events = await commandHandler.handle(
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

        const events = (_events instanceof Array ? _events : [_events]).map((item, index) => ({
          ...item,
          id: new EventId(),
          timestamp,
          aggregate: {
            id: this.id,
            version: this.version + index + 1,
          },
          meta: item.meta ?? {},
        }));

        await this.eventStore.dispatchEvents({
          aggregate: {
            id: this.id,
            version: this.version + 1,
          },
          events: events.map(item => ({
            id: item.id,
            type: item.type,
            body: item.body,
            meta: item.meta,
          })),
          timestamp,
        });

        await this.digest(events);
      } finally {
        release();
      }
    }, {
      delayFirstAttempt: false,
      jitter: 'full',
      maxDelay: 2000,
      numOfAttempts: opts?.maxRetries || 10,
      startingDelay: 100,
      timeMultiple: 2,
      retry(err) {
        return err instanceof AggregateVersionConflictError;
      },
    });
  }
}