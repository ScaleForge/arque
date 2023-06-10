import { Event, EventStoreStorageAdapter, Snapshot } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import { EventSchema } from './lib/schema';

const SCHEMAS = {
  Event: EventSchema,
};

export class MongooseEventStoreStorageAdapter implements EventStoreStorageAdapter {
  private connectionPromise: Promise<Connection>;

  constructor(private readonly opts?: {
    readonly uri?: string;
  } & Pick<ConnectOptions, 'maxPoolSize' | 'minPoolSize' | 'socketTimeoutMS' | 'serverSelectionTimeoutMS'>) {
  }

  private async connection() {
    if (!this.connectionPromise) {
      this.connectionPromise = mongoose.createConnection(this.opts?.uri ?? 'mongodb://localhost:27017/arque', {
        writeConcern: {
          w: 1,
        },
        readPreference: 'secondaryPreferred',
        minPoolSize: this.opts?.maxPoolSize ?? 2,
        maxPoolSize: this.opts?.maxPoolSize ?? 10,
        socketTimeoutMS: this.opts?.socketTimeoutMS ?? 45000,
        serverSelectionTimeoutMS: this.opts?.serverSelectionTimeoutMS ?? 10000,
      }).asPromise();
    }

    return this.connectionPromise;
  }

  private async model(model: keyof typeof SCHEMAS) {
    const connection = await this.connection();

    return connection.model(model, SCHEMAS[model]);
  }

  async saveEvents(params: {
    aggregate: { id: Buffer; version: number; };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
  }): Promise<{ commit(): Promise<void>; abort(): Promise<void>; }> {
    const EventModel = await this.model('Event');

    const handle = async () => {
      const session = await EventModel.startSession();

      session.startTransaction({
        writeConcern: {
          w: 'majority',
        },
      });

      try {
        await EventModel.insertMany(params.events.map(item => ({
          ...item,
          aggregate: params.aggregate,
          timestamp: params.timestamp,
        })), { session });
      } catch (err) {
        try {
          await session.abortTransaction();
        } finally {
          await session.endSession();
        }
        
        throw err;
      }

      return session;
    };

    const session = await handle();

    return {
      async commit() {
        try {
          await session.commitTransaction();
        } finally {
          await session.endSession();
        }
      },
      async abort() {
        try {
          await session.abortTransaction();
        } finally {
          await session.endSession();
        }
      },
    };
  }

  async listEvents<TEvent = Event>(_params: { aggregate: { id: Buffer; version?: number; }; }): Promise<AsyncIterableIterator<TEvent>> {
    throw new Error('Method not implemented.');
  }

  async saveSnapshot<TState = unknown>(_params: Snapshot<TState>): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async getLatestSnapshot<TState = unknown>(_params: { aggregate: { id: Buffer; version: number; }; }): Promise<Snapshot<TState>> {
    throw new Error('Method not implemented.');
  }

  async registerStream(_params: { event: number; stream: string; }): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async close(): Promise<void> {
    if (this.connectionPromise) {
      const connection = await this.connectionPromise;

      await connection.close();
    }
  }
}