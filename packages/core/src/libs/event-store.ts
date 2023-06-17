import { Joser } from '@scaleforge/joser';
import { StorageAdapter } from './storage-adapter';
import { Event, Snapshot } from './types';

export interface EventStoreStreamReceiver {
  stop(): Promise<void>;
}

export interface EventStoreStreamAdapter {
  sendEvents(params: { events: Event[] }): Promise<void>;
  receiveEvents(
    stream: string,
    handler: (event: Event) => Promise<void>
  ): Promise<EventStoreStreamReceiver>;
}

export type Serializer = {
  serialize: (value: unknown) => unknown;
  deserialize: (raw: unknown) => unknown;
};

export class EventStore {
  private readonly serializer: Serializer;

  constructor(
    private readonly storageAdapter: StorageAdapter,
    private readonly streamAdapter: EventStoreStreamAdapter,
    opts?: {
      serializer?: Serializer
    },
  ) {
    this.serializer = opts?.serializer ?? new Joser();
  }

  public async saveSnapshot<TState = unknown>(params: Snapshot<TState>): Promise<void> {
    await this.storageAdapter.saveSnapshot(params);
  }

  public async getSnapshot<TState = unknown>(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
  }): Promise<Snapshot<TState> | null> {
    return this.storageAdapter.getSnapshot(params);
  }

  public async listEvents<TEvent = Event>(
    params: {
      aggregate: {
        id: Buffer;
        version?: number;
      };
    }
  ): Promise<AsyncIterableIterator<TEvent>> {
    return this.storageAdapter.listEvents(params);
  }

  public async saveEvents(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
  }) {
    const events: Event[] = await Promise.all(params.events.map(async (event, index) => {
      const streams = await this.storageAdapter.listStreams({ event: event.type });

      return {
        ...event,
        aggregate: {
          id: params.aggregate.id,
          version: params.aggregate.version + index,
        },
        body: this.serializer.serialize(event.body),
        meta: {
          ...event.meta,
          streams,
        },
        timestamp: params.timestamp,
      };
    }));

    await this.storageAdapter.saveEvents(params);
    await this.streamAdapter.sendEvents({ events });
  }
}