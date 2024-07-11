import assert from 'assert';
import { ConfigAdapter, StoreAdapter, StreamAdapter, Subscriber } from './adapters';
import { ProjectionEventHandler, Event } from './types';
import debug from 'debug';
import { inspect } from 'util';

export class Projection<
  TState = unknown,
  TEventHandler extends ProjectionEventHandler<Event, TState> = ProjectionEventHandler<Event, TState>,
> {
  protected readonly logger = {
    info: debug('info:Projection'),
    error: debug('error:Projection'),
    warn: debug('warn:Projection'),
    verbose: debug('verbose:Projection'),
    debug: debug('debug:Projection'),
  };

  private readonly eventHandlers: Map<
    number,
    TEventHandler
  >;

  private subscriber: Subscriber | null = null;

  private timestampLastEventReceived = Date.now();

  constructor(
    private readonly store: StoreAdapter,
    private readonly stream: StreamAdapter,
    private readonly config: ConfigAdapter,
    eventHandlers: TEventHandler[],
    private _id: string,
    private readonly _state: TState,
    private readonly opts?: {
      disableSaveStream?: true;
    }
  ) {
    this.eventHandlers = new Map(
      eventHandlers.map(item => [item.type, item])
    );
  }

  get id() {
    return this._id;
  }

  get state() {
    return this._state;
  }

  private async handleEvent(event: Omit<Event, 'body'> & { body: Record<string, unknown> | null }) {
    const timestamp = new Date();

    this.timestampLastEventReceived = Date.now();

    const handler = this.eventHandlers.get(event.type);

    assert(handler, `handler does not exist: event=${event.type}`);

    const { handle } = handler;
    
    if (await this.store.checkProjectionCheckpoint({ projection: this.id, aggregate: event.aggregate })) {
      try {
        await handle({ state: this._state }, event);
      } catch (err) {
        this.logger.error(`error occured while handling event: error="${err.message}" event="${
          inspect({
            id: event.id.toString(),
            type: event.type,
          }, {
            breakLength: Infinity,
            compact: true,
          })
        }"`);

        throw err;
      }

      await this.store.saveProjectionCheckpoint({
        projection: this.id,
        aggregate: event.aggregate,
      });

      this.logger.verbose(`event handled: event="${
        inspect({
          id: event.id.toString(),
          type: event.type,
        }, {
          breakLength: Infinity,
          compact: true,
        })
      }" duration=${Date.now() - timestamp.getTime()}ms`);
    }
  }

  async waitUntilSettled(duration: number = 60000) {
    while (Date.now() - this.timestampLastEventReceived < duration) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  async start() {
    if (this.subscriber) {
      throw new Error('already started');
    }

    if (!this.opts?.disableSaveStream) {
      await this.config.saveStream({
        id: this.id,
        events: [...new Set([...this.eventHandlers.values()].map(item => item.type)).values()],
      });
    }

    this.subscriber = await this.stream.subscribe(
      this.id,
      async (event) => {
        await this.handleEvent(event as never);
      },
    );
  }

  async stop(): Promise<void> {
    if (this.subscriber) {
      await this.subscriber.stop();
    }
  }
}