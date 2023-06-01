/* eslint-disable @typescript-eslint/no-unused-vars */
import { Event, EventStoreStorageAdapter, Snapshot, EventId } from '@arque/core';
import mongoose, { Connection, ConnectOptions, Model, Schema } from 'mongoose';

const EventSchema = new Schema({
  _id: Buffer,
  type: Number,
  aggregate: {
    id: Buffer,
    version: Number,
  },
  body: Schema.Types.Mixed,
  timestamp: Date,
}, {
  id: false,
  autoIndex: true,
  virtuals: {
    id: {
      get() {
        return EventId.from(this._id);
      },
      set(value: EventId) {
        this._id = value.buffer;
      },
    },
  },
});
EventSchema.index({ 'aggregate.id': 1, 'aggregate.version': 1 }, { unique: true });

const MODEL_SCHEMA_MAP = {
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

  private async model(model: keyof typeof MODEL_SCHEMA_MAP) {
    const connection = await this.connection();

    return connection.model(model, MODEL_SCHEMA_MAP[model]);
  }

  saveEvents(params: { aggregate: { id: Buffer; version: number; }; timestamp: Date; events: Pick<Event, 'id' | 'type' | 'body'>[]; }): Promise<{ commit(): Promise<void>; abort(): Promise<void>; }> {
    throw new Error('Method not implemented.');
  }
  listEvents<TEvent = Event>(params: { aggregate: { id: Buffer; version?: number; }; }): Promise<AsyncIterableIterator<TEvent>> {
    throw new Error('Method not implemented.');
  }
  saveSnapshot<TState = unknown>(params: Snapshot<TState>): Promise<void> {
    throw new Error('Method not implemented.');
  }
  getLatestSnapshot<TState = unknown>(params: { aggregate: { id: Buffer; version: number; }; }): Promise<Snapshot<TState>> {
    throw new Error('Method not implemented.');
  }
  registerStream(params: { event: number; stream: string; }): Promise<void> {
    throw new Error('Method not implemented.');
  }
}