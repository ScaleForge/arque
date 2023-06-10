import { Event, EventStoreStorageAdapter, Snapshot } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import { EventSchema } from './lib/schema';
import { backOff } from 'exponential-backoff';

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
  }): Promise<void> {
    const EventModel = await this.model('Event');

    await backOff(async () => {
      const session = await EventModel.startSession();

      session.startTransaction({
        writeConcern: {
          w: 'majority',
        },
      });

      try {
        try {
          await EventModel.insertMany(params.events.map(item => ({
            ...item,
            aggregate: params.aggregate,
            timestamp: params.timestamp,
          })), { session });
        } catch (err) {
          await session.abortTransaction();
          
          throw err;
        }
  
        await session.commitTransaction();
      } finally {
        await session.endSession();
      }
    }, {
      startingDelay: 100,
      jitter: 'full',
      maxDelay: 800,
      numOfAttempts: 4,
    });
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
