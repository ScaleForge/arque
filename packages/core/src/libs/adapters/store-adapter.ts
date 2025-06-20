import { Event } from '../types';

export class AggregateVersionConflictError extends Error {
  constructor(id: Buffer, version: number) {
    super(
      `aggregate version conflict: id=${id.toString('hex')} version=${version}`
    );
  }
}

export class AggregateIsFinalError extends Error {
  constructor(id: Buffer) {
    super(`aggregate is final: id=${id.toString('hex')}`);
  }
}

export type Snapshot<TState = unknown> = {
  aggregate: {
    id: Buffer;
    version: number;
  };
  state: TState;
  timestamp: Date;
};

export interface StoreAdapter {
  init(): Promise<void>;

  saveEvents(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
    meta?: Event['meta'];
  }): Promise<void>;

  listEvents<TEvent = Event>(params: {
    aggregate: {
      id: Buffer;
      version?: number;
    };
  }): Promise<AsyncIterableIterator<TEvent>>;

  listEvents<TEvent = Event>(params: {
    type: number;
  }): Promise<AsyncIterableIterator<TEvent>>;

  saveSnapshot(params: Snapshot): Promise<void>;

  findLatestSnapshot<TState = unknown>(params: {
    aggregate: {
      id: Buffer;
      version: number;
    };
  }): Promise<Snapshot<TState> | null>;

  saveProjectionCheckpoint(params: {
    projection: string;
    aggregate: {
      id: Buffer;
      version: number;
    };
  }): Promise<void>;

  checkProjectionCheckpoint(params: {
    projection: string;
    aggregate: {
      id: Buffer;
      version: number;
    };
  }): Promise<boolean>;

  finalizeAggregate(params: {
    id: Buffer;
  }): Promise<void>;

  close(): Promise<void>;
}
