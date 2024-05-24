import assert from 'assert';
import { ConfigAdapter, StoreAdapter, StreamAdapter, Subscriber } from './adapters';
import { ProjectionEventHandler, Event } from './types';
import debug from 'debug';

export class Projection<
  TState = unknown,
  TEventHandler extends ProjectionEventHandler<Event, TState> = ProjectionEventHandler<Event, TState>,
> {
  protected readonly logger = {
    info: debug('broker:info'),
    error: debug('broker:error'),
    warn: debug('broker:warn'),
    verbose: debug('broker:verbose'),
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
    this.timestampLastEventReceived = Date.now();

    const handler = this.eventHandlers.get(event.type);

    assert(handler, `handler does not exist: event=${event.type}`);

    const { handle } = handler;
    
    if (await this.store.checkProjectionCheckpoint({ projection: this.id, aggregate: event.aggregate })) {
      await handle({ state: this._state }, event);

      await this.store.saveProjectionCheckpoint({
        projection: this.id,
        aggregate: event.aggregate,
      });
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