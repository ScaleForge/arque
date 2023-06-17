import { Event, Snapshot, Stream } from './types';

export interface StorageAdapter {
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

  getSnapshot<TState = unknown>(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
  }): Promise<Snapshot<TState> | null>;

  saveStream(params: Stream): Promise<void>;

  listStreams(params: { event: number }): Promise<string[]>;
}