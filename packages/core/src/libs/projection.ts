import assert from 'assert';
import { EventStore } from './event-store';
import { Event } from './types';

export type ProjectionEventHandler<TEvent extends Event, TState> = {
  type: number;
  handle(ctx: { readonly state: TState }, event: TEvent): void | Promise<void>;
};

export class Projection<TEvent extends Event, TState> {
  private readonly eventHandlers: Map<
    number,
    ProjectionEventHandler<TEvent, TState>
  >;

  private subscriber: {
    stop(): Promise<void>;
  } | null = null;

  constructor(
    private readonly eventStore: EventStore,
    eventHandlers: ProjectionEventHandler<TEvent, TState>[],
    private readonly id: string,
    private readonly state: TState,
  ) {
    this.eventHandlers = new Map(
      eventHandlers.map((item) => [item.type, item])
    );
  }

  private getEventHandler(type: number) {
    const handler = this.eventHandlers.get(type);

    assert(handler, `event handler does not exist: type=${type}`);

    return handler;
  }

  private async handleEvent(event: TEvent) {
    const { handle } = this.getEventHandler(event.type);

    await handle({
      state: this.state,
    }, event);
  }

  public async start() {
    await this.eventStore.saveStream({
      name: this.id,
      events: Array.from(this.eventHandlers.keys()),
    });

    this.subscriber = await this.eventStore.subscribe<TEvent>({
      stream: this.id,
      handle: async (event) => {
        await this.handleEvent(event);
      },
    });
  }

  public async stop() {
    if (this.subscriber) {
      await this.subscriber.stop();
    }
  }
}