import { Joser } from '@scaleforge/joser';
import { StorageAdapter } from './storage-adapter';
import { Event, Snapshot } from './types';
import { StreamAdapter } from './stream-adapter';

export type Serializer = {
  serialize: (value: unknown) => unknown;
  deserialize: (raw: unknown) => unknown;
};

export class EventStore {
  private readonly serializer: Serializer;

  constructor(
    private readonly storageAdapter: StorageAdapter,
    private readonly streamAdapter: StreamAdapter,
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

  public async dispatchEvents(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
    meta?: Event['meta']
  }) {
    const events: Event[] = params.events.map((event, index) => {
      return {
        ...event,
        aggregate: {
          id: params.aggregate.id,
          version: params.aggregate.version + index,
        },
        body: this.serializer.serialize(event.body),
        meta: {
          ...event.meta,
          ...params.meta,
        },
        timestamp: params.timestamp,
      };
    });

    await this.storageAdapter.saveEvents(params);
    await this.streamAdapter.sendEvents({ events });
  }
}