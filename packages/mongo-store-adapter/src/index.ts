/** build x1 */
import { Event, StoreAdapter, Snapshot, AggregateVersionConflictError, EventId } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import * as schema from './libs/schema';
import { backOff } from 'exponential-backoff';
import { Joser, Serializer } from '@scaleforge/joser';
import debug from 'debug';
import assert from 'assert';
import Queue from 'p-queue';
import { match, P } from 'ts-pattern';

type Options = {
  readonly uri: string;
  readonly retryStartingDelay: number;
  readonly retryMaxDelay: number;
  readonly retryMaxAttempts: number;
  readonly serializers: Serializer[];
} & Readonly<Pick<ConnectOptions, 'maxPoolSize' | 'minPoolSize' | 'socketTimeoutMS' | 'serverSelectionTimeoutMS'>>;

export type MongoStoreAdapterOptions = Partial<Options>;

export class MongoStoreAdapter implements StoreAdapter {
  private readonly logger = {
    info: debug('info:MongoStoreAdapter'),
    error: debug('error:MongoStoreAdapter'),
    warn: debug('warn:MongoStoreAdapter'),
    verbose: debug('verbose:MongoStoreAdapter'),
    debug: debug('debug:MongoStoreAdapter'),
  };

  private readonly joser: Joser;

  private readonly opts: Options;

  private readonly saveSnapshotQueue = new Queue({
    autoStart: true,
    concurrency: 1,
  });

  private _connection: Promise<Connection>;

  private _init: Promise<void>;

  constructor(opts?: Partial<Options>) {
    const maxPoolSize = opts?.maxPoolSize ?? 100;

    this.opts = {
      uri: opts?.uri ?? 'mongodb://localhost:27017/arque',
      retryStartingDelay: opts?.retryStartingDelay ?? 100,
      retryMaxDelay: opts?.retryMaxDelay ?? 1600,
      retryMaxAttempts: opts?.retryMaxAttempts ?? 10,
      maxPoolSize,
      minPoolSize: opts?.minPoolSize ?? Math.floor(maxPoolSize * 0.2),
      socketTimeoutMS: opts?.socketTimeoutMS ?? 45000,
      serverSelectionTimeoutMS: opts?.serverSelectionTimeoutMS ?? 25000,
      serializers: opts?.serializers ?? [],
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
        ...(this.opts.serializers),
      ],
    });
  }

  private serialize(value) {
    return match(value)
      .with(P.union(P.nullish, P.number, P.string, P.boolean, P.instanceOf(Buffer)), (value) => value)
      .otherwise((value) => this.joser.serialize(value));
  }

  private deserialize(value) {
    return match(value)
      .with(P.union(P.nullish, P.number, P.string, P.boolean, P.instanceOf(Buffer)), (value) => value)
      .otherwise((value) => this.joser.deserialize(value));
  }

  public async init() {
    if (!this._init) {
      this._init = (async () => {
        await this.connection();
      })().catch((err) => {
        delete this._init;

        throw err;
      });
    }

    await this._init;
  }

  private async connection() {
    if (!this._connection) {
      this._connection = (async () => {
        const connection = await mongoose.createConnection(this.opts.uri, {
          maxPoolSize: this.opts.maxPoolSize,
          minPoolSize: this.opts.minPoolSize,
          socketTimeoutMS: this.opts?.socketTimeoutMS,
          serverSelectionTimeoutMS: this.opts?.serverSelectionTimeoutMS,
        }).asPromise();

        return connection
      })().catch((err) => {
        delete this._connection;

        throw err;
      });
    }

    return this._connection;
  }

  public async model(model: keyof typeof schema) {
    const connection = await this.connection();;

    return connection.model(model, schema[model]);
  }

  async saveProjectionCheckpoint(params: { projection: string; aggregate: { id: Buffer; version: number; }; }): Promise<void> {
    const ProjectionCheckpointModel = await this.model('ProjectionCheckpoint');

    await ProjectionCheckpointModel.updateOne(
      { projection: params.projection, 'aggregate.id': params.aggregate.id },
      {
        $set: {
          'aggregate.version': params.aggregate.version,
        },
        $setOnInsert: {
          projection: params.projection,
          'aggregate.id': params.aggregate.id,
          timestamp: new Date(),
        },
      },
      {
        upsert: true,
        writeConcern: {
          w: 1,
        }
      },
    );
  }

  async checkProjectionCheckpoint(params: { projection: string; aggregate: { id: Buffer; version: number; }; }): Promise<boolean> {
    const ProjectionCheckpointModel = await this.model('ProjectionCheckpoint');
    
    const count = await ProjectionCheckpointModel.countDocuments({
      projection: params.projection,
      'aggregate.id': params.aggregate.id,
      'aggregate.version': { $gte: params.aggregate.version },
    }, {
      limit: 1,
      readPreference: 'primaryPreferred',
    });

    return count === 0;
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

    await backOff(async () => {
      const session = await EventModel.startSession();

      session.startTransaction({
        writeConcern: {
          w: 1,
        },
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
          const { modifiedCount: count } = await AggregateModel.updateOne(
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

          if (count === 0) {
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
          body: this.serialize(event.body),
          meta: this.serialize({
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
      retry: (err) => {
        const retry = [
          'SnapshotUnavailable',
          'NotWritablePrimary',
          'LockTimeout',
          'NoSuchTransaction',
          'InterruptedDueToReplStateChange',
          'WriteConflict',
        ].includes(err.codeName);
        
        if (retry) {
          this.logger.warn('retry #saveEvents: code=%s', err.codeName);
        } else {
          this.logger.error('error #saveEvents: message=%s', err.message);
        }

        return retry;
      },
    });
  }

  async listEvents<TEvent = Event>(params: {
    aggregate: {
      id: Buffer;
      version?: number;
    };
  }): Promise<AsyncIterableIterator<TEvent>>;

  async listEvents<TEvent = Event>(params: {
    type: number;
  }): Promise<AsyncIterableIterator<TEvent>> 

  async listEvents<TEvent = Event>(params: {
    aggregate?: {
      id: Buffer;
      version?: number;
    };
    type?: number;
  }): Promise<AsyncIterableIterator<TEvent>> {
    const _this = this;
    
    const EventModel = await this.model('Event');

    let query = {};

    let sort = {};

    if (params.aggregate) {
      query = {
        'aggregate.id': params.aggregate.id,
        'aggregate.version': { $gt: params.aggregate.version ?? 0 },
      };

      sort = {
        'aggregate.id': 1,
        'aggregate.version': 1,
      };
    }
    
    if (typeof params.type === 'number') {
      query = {
        'type': params.type,
      };

      sort = {
        'type': 1,
        'timestamp': 1,
      };
    }

    const cursor = EventModel.find(query, null, {
      readPreference: 'primaryPreferred',
    }).sort({ 'aggregate.id': 1, 'aggregate.version': 1 }).cursor({
      batchSize: 256,
    });
    
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
            body: _this.deserialize(doc['body'] ?? {}),
            meta: _this.deserialize(doc['meta'] ?? {}),
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

  async _saveSnapshot(params: Snapshot) {
    const SnapshotModel = await this.model('Snapshot');

    await SnapshotModel.create([{
      ...params,
      state: this.serialize(<never>params.state),
    }], {
      validateBeforeSave: false,
      w: 1
    });
  }

  async saveSnapshot(params: Snapshot) {
    await this.saveSnapshotQueue.add(() => this._saveSnapshot(params));
  }

  async findLatestSnapshot<T = unknown>(params: { aggregate: { id: Buffer; version: number; }; }): Promise<Snapshot<T> | null> {
    const SnapshotModel = await this.model('Snapshot');

    const snapshot = await SnapshotModel.findOne({
      'aggregate.id': params.aggregate.id,
      'aggregate.version': { $gt: params.aggregate.version },
    }, null, {
      readPreference: 'secondary',
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
      state: this.deserialize(snapshot['state']) as T,
      timestamp: snapshot['timestamp'],
    };
  }

  async close(): Promise<void> {
    await this.saveSnapshotQueue.onIdle();

    if (this._connection) {
      const connection = await this._connection;

      await connection.close();
    }
  }
}
