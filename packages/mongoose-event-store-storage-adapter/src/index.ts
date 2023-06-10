import { Event, EventStoreStorageAdapter, Snapshot, EventId } from '@arque/core';
import mongoose, { Connection, ConnectOptions, Schema } from 'mongoose';

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

  async saveEvents(params: {
    aggregate: { id: Buffer; version: number; };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
  }): Promise<{ commit(): Promise<void>; abort(): Promise<void>; }> {
    const EventModel = await this.model('Event');

    const session = await EventModel.startSession();

    session.startTransaction({
      writeConcern: {
        w: 'majority',
      },
    });

    const commit = async () => {
      try {
        await session.commitTransaction();
      } finally {
        await session.endSession(); 
      }
    };

    const abort = async () => {
      await session.abortTransaction();
      await session.endSession();
    };

    try {
      await EventModel.insertMany(params.events.map(item => ({
        ...item,
        aggregate: params.aggregate,
        timestamp: params.timestamp,
      })), { session });
    } catch (err) {
      await abort();
      
      throw err;
    }

    return {
      commit,
      abort,
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