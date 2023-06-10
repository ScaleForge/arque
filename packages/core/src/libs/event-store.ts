/* eslint-disable @typescript-eslint/no-unused-vars */
import { Event, Snapshot } from './types';

export interface EventStoreStreamReceiver {
  stop(): Promise<void>;
}

export interface EventStoreStreamAdapter {
  sendEvents(data: { streams: string[], event: Event }[]): Promise<void>;
  receiveEvents(
    stream: string,
    handler: (event: Event) => Promise<void>
  ): Promise<EventStoreStreamReceiver>;
}

export interface EventStoreStorageAdapter {
  saveEvents(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
  }): Promise<void>;

  listEvents<TEvent = Event>(params: {
    aggregate: {
      id: Buffer;
      version?: number;
    };
  }): Promise<AsyncIterableIterator<TEvent>>;

  saveSnapshot<TState = unknown>(params: Snapshot<TState>): Promise<void>;

  getLatestSnapshot<TState = unknown>(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
  }): Promise<Snapshot<TState> | null>;

  registerStream(params: { event: number; stream: string; }): Promise<void>;
}

export interface EventStoreConfigurationStorageAdapter {
  listStreams(params: { event: number }): Promise<string[]>
}

export class EventStore {
  constructor(
    private readonly storageAdapter: EventStoreStorageAdapter,
    private readonly streamAdapter: EventStoreStreamAdapter,
    private readonly configurationStorageAdapter: EventStoreConfigurationStorageAdapter
  ) {}

  public async saveSnapshot<TState = unknown>(params: Snapshot<TState>): Promise<void> {
    await this.storageAdapter.saveSnapshot(params);
  }

  public async getLatestSnapshot<TState = unknown>(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
  }): Promise<Snapshot<TState> | null> {
    return this.storageAdapter.getLatestSnapshot(params);
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
    const events: Event[] = params.events.map((item, index) => ({
      ...item,
      aggregate: {
        id: params.aggregate.id,
        version: params.aggregate.version + index,
      },
      timestamp: params.timestamp,
    }));

    const streamAdapterSendEventsData = await Promise.all(events.map(async (event) => {
      const streams = await this.configurationStorageAdapter.listStreams({ event: event.type });

      return { streams, event };
    }));

    await this.storageAdapter.saveEvents(params);
    await this.streamAdapter.sendEvents(streamAdapterSendEventsData);
  }
}