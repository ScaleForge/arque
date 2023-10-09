import { Event, StoreAdapter, Snapshot, AggregateVersionConflictError, EventId } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import * as schema from './libs/schema';
import { backOff } from 'exponential-backoff';
import { Joser, Serializer } from '@scaleforge/joser';
import debug from 'debug';
import assert from 'assert';

type Options = {
  readonly uri: string;
  readonly retryStartingDelay: number;
  readonly retryMaxDelay: number;
  readonly retryMaxAttempts: number;
} & Readonly<Pick<ConnectOptions, 'maxPoolSize' | 'minPoolSize' | 'socketTimeoutMS' | 'serverSelectionTimeoutMS'>>;

export class MongoStoreAdapter implements StoreAdapter {
  private readonly logger = {
    info: debug('MongoStoreAdapter:info'),
    error: debug('MongoStoreAdapter:error'),
    warn: debug('MongoStoreAdapter:warn'),
    verbose: debug('MongoStoreAdapter:verbose'),
  };

  private readonly joser: Joser;

  private readonly opts: Options;

  private connectionPromise: Promise<Connection>;

  constructor(opts?: Partial<Options & { readonly serializers: Serializer[]; }>) {
    this.opts = {
      uri: opts?.uri ?? 'mongodb://localhost:27017/arque',
      retryStartingDelay: opts?.retryStartingDelay ?? 100,
      retryMaxDelay: opts?.retryMaxDelay ?? 1600,
      retryMaxAttempts: opts?.retryMaxAttempts ?? 10,
      maxPoolSize: opts?.maxPoolSize ?? 10,
      minPoolSize: opts?.minPoolSize ?? 2,
      socketTimeoutMS: opts?.socketTimeoutMS ?? 45000,
      serverSelectionTimeoutMS: opts?.serverSelectionTimeoutMS ?? 10000,
    };

    this.joser = new Joser({
      serializers: [
        {
          type: Buffer,
          serialize: (value: Buffer) => value,
          deserialize: (value: { buffer: Buffer }) => Buffer.from(value.buffer),
        },
        {
          type: Date,
          serialize: (value: Date) => value,
          deserialize: (value: Date) => value,
        },
        ...(opts?.serializers ?? []),
      ],
    });
  }

  private async connection() {
    if (!this.connectionPromise) {
      this.connectionPromise = mongoose.createConnection(this.opts.uri, {
        writeConcern: {
          w: 1,
        },
        readPreference: 'secondaryPreferred',
        minPoolSize: this.opts.maxPoolSize,
        maxPoolSize: this.opts.maxPoolSize,
        socketTimeoutMS: this.opts?.socketTimeoutMS,
        serverSelectionTimeoutMS: this.opts?.serverSelectionTimeoutMS,
      }).asPromise().catch((err) => {
        delete this.connectionPromise;

        throw err;
      });
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
    meta?: Event['meta'];
  }): Promise<void> {
    assert(params.aggregate.version > 0, 'aggregate version must be greater than 0');

    const [EventModel, AggregateModel] = await Promise.all([
      this.model('Event'),
      this.model('Aggregate'),
    ]);

    const { logger } = this;

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
          _id: event.id.buffer,
          type: event.type,
          aggregate: {
            id: params.aggregate.id,
            version: params.aggregate.version + index,
          },
          body: this.joser.serialize(event.body),
          meta: this.joser.serialize({
            ...event.meta,
            ...params.meta,
          }),
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
      startingDelay: this.opts.retryStartingDelay,
      maxDelay: this.opts.retryMaxDelay,
      numOfAttempts: this.opts.retryMaxAttempts,
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

  saveSnapshot(params: Snapshot<unknown>): Promise<void> {
    console.log(params);
    throw new Error('Method not implemented.');
  }

  findLatestSnapshot<TState = unknown>(params: { aggregate: { id: Buffer; version: number; }; }): Promise<Snapshot<TState>> {
    console.log(params);
    throw new Error('Method not implemented.');
  }

  async listEvents<TEvent = Event>(params: { aggregate: { id: Buffer; version?: number; }; }): Promise<AsyncIterableIterator<TEvent>> {
    const EventModel = await this.model('Event');

    const cursor = EventModel.find({
      'aggregate.id': params.aggregate.id,
      'aggregate.version': { $gt: params.aggregate.version ?? 0 },
    }).cursor({
      batchSize: 100,
    });

    const { joser } = this;

    return {
      async next() {
        const doc = await cursor.next();

        if (!doc) {
          return { done: true };
        }

        return {
          value: {
            id: EventId.from(Buffer.from(doc['_id'] as never)),
            type: doc['type'],
            aggregate: {
              id: Buffer.from(doc['aggregate']['id']),
              version: doc['aggregate']['version'],
            },
            body: joser.deserialize(doc['body'] ?? {}),
            meta: joser.deserialize(doc['meta'] ?? {}),
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
  /**
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
   */
  async close(): Promise<void> {
    if (this.connectionPromise) {
      const connection = await this.connectionPromise;

      await connection.close();
    }
  }

}
