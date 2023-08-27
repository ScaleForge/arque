import { Event, StorageAdapter, Snapshot, AggregateVersionConflictError } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import * as schema from './libs/schema';
import { backOff } from 'exponential-backoff';
import debug from 'debug';
import assert from 'assert';

const logger = {
  warn: debug('MongoStorageAdapter:WARN'),
  error: debug('MongoStorageAdapter:ERROR'),
};

export class MongoStorageAdapter implements StorageAdapter {
  private connectionPromise: Promise<Connection>;

  constructor(private readonly opts?: {
    readonly uri?: string;
    readonly saveEventsRetryStartingDelay?: number;
    readonly saveEventsRetryMaxDelay?: number;
    readonly saveEventsRetryMaxAttempts?: number;
  } & Readonly<Pick<ConnectOptions, 'maxPoolSize' | 'minPoolSize' | 'socketTimeoutMS' | 'serverSelectionTimeoutMS'>>) {}

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

  private async model(model: keyof typeof schema) {
    const connection = await this.connection();

    return connection.model(model, schema[model]);
  }

  async saveEvents(params: {
    aggregate: { id: Buffer; version: number; };
    timestamp: Date;
    events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
  }): Promise<void> {
    assert(params.aggregate.version > 0, 'aggregate version must be greater than 0');

    const [EventModel, AggregateModel] = await Promise.all([
      this.model('Event'),
      this.model('Aggregate'),
    ]);

    await backOff(async () => {
      const session = await EventModel.startSession();

      session.startTransaction({
        writeConcern: {
          w: 'majority',
        },
        readPreference: 'primary',
        retryWrites: true,
      });

      try {
        if (params.aggregate.version === 1) {
          try {
            await AggregateModel.create(
              [
                {
                  _id: params.aggregate.id,
                  version: params.events.length,
                  timestamp: params.timestamp,
                },
              ],
              { session }
            );
          } catch (err) {
            if (err.name === 'MongoServerError' && err.code === 11000) {
              throw new AggregateVersionConflictError(params.aggregate.id, params.aggregate.version);
            }

            throw err;
          }
        } else {
          const { modifiedCount } = await AggregateModel.updateOne(
            {
              _id: params.aggregate.id,
              version: params.aggregate.version - 1,
            },
            {
              $set: {
                version: params.aggregate.version + params.events.length - 1,
                timestamp: params.timestamp,
              },
            },
            {
              session,
            }
          );

          if (modifiedCount === 0) {
            throw new AggregateVersionConflictError(params.aggregate.id, params.aggregate.version);
          }
        }

        await EventModel.insertMany(params.events.map((event, index) => ({
          ...event,
          aggregate: {
            id: params.aggregate.id,
            version: params.aggregate.version + index,
          },
          timestamp: params.timestamp,
        })), { session });

      } catch(err) {
        await session.abortTransaction();
        await session.endSession();

        throw err;
      }

      try {
        await session.commitTransaction();
      } finally {
        await session.endSession();
      }
    }, {
      startingDelay: this.opts?.saveEventsRetryStartingDelay ?? 100,
      maxDelay: this.opts?.saveEventsRetryMaxDelay ?? 1600,
      numOfAttempts: this.opts?.saveEventsRetryMaxAttempts ?? 10,
      jitter: 'full',
      retry(err) {
        const retry = [
          'SnapshotUnavailable',
          'NotWritablePrimary',
          'LockTimeout',
          'NoSuchTransaction',
          'InterruptedDueToReplStateChange',
          'WriteConflict',
        ].includes(err.codeName);
        
        if (retry) {
          logger.warn('retry #saveEvents: code=%s', err.codeName);
        } else {
          logger.error('error #saveEvents: message=%s', err.message);
        }

        return retry;
      },
    });
  }

  async listEvents<TEvent = Event>(params: { aggregate: { id: Buffer; version?: number; }; }): Promise<AsyncIterableIterator<TEvent>> {
    const Event = await this.model('Event');

    const cursor = Event.find({
      'aggregate.id': params.aggregate.id,
      'aggregate.version': { $gt: params.aggregate.version ?? 0 },
    }).cursor({
      batchSize: 100,
    });

    return {
      async next() {
        const doc = await cursor.next();

        if (!doc) {
          return { done: true };
        }

        return {
          value: {
            id: doc['id'],
            type: doc['type'],
            aggregate: {
              id: Buffer.from(doc['aggregate']['id']),
              version: doc['aggregate']['version'],
            },
            body: doc['body'],
            meta: doc['meta'] ?? {},
            timestamp: doc['timestamp'],
          },
          done: false,
        };
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    } as never;
  }

  async saveSnapshot<TState = unknown>(params: Snapshot<TState>): Promise<void> {
    const Snapshot = await this.model('Snapshot');

    await Snapshot.create([params], {
      validateBeforeSave: false,
      w: 1,
    });
  }

  async getSnapshot<TState = unknown>(params: { aggregate: { id: Buffer; version: number; }; }): Promise<Snapshot<TState> | null> {
    const Snapshot = await this.model('Snapshot');

    const snapshot = await Snapshot.findOne({
      'aggregate.id': params.aggregate.id,
      'aggregate.version': { $gt: params.aggregate.version },
    }).sort({
      'aggregate.version': -1,
    });
    
    if (!snapshot) {
      return null;
    }

    return {
      aggregate: {
        id: Buffer.from(snapshot['aggregate']['id']),
        version: snapshot['aggregate']['version'],
      },
      state: snapshot['state'],
      timestamp: snapshot['timestamp'],
    };
  }

  async close(): Promise<void> {
    if (this.connectionPromise) {
      const connection = await this.connectionPromise;

      await connection.close();
    }
  }
}
