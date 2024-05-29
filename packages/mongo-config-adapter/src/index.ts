import { ConfigAdapter } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import * as schema from './libs/schema';
import debug from 'debug';
import { LRUCache } from 'lru-cache';

type Options = {
  readonly uri: string;
  readonly cacheMax: number;
  readonly cacheTTL: number;
} & Readonly<Pick<ConnectOptions, 'maxPoolSize' | 'minPoolSize' | 'socketTimeoutMS' | 'serverSelectionTimeoutMS'>>;

export type MongoConfigAdapterOptions = Partial<Options>;

export class MongoConfigAdapter implements ConfigAdapter {
  private readonly logger = {
    info: debug('info:MongoStoreAdapter'),
    error: debug('error:MongoStoreAdapter'),
    warn: debug('warn:MongoStoreAdapter'),
    verbose: debug('verbose:MongoStoreAdapter'),
  };

  private readonly opts: Options;

  private readonly cache: LRUCache<number, string[]>;

  private _connection: Promise<Connection>;

  private _init: Promise<void>;

  constructor(opts?: Partial<Options>) {
    this.opts = {
      uri: opts?.uri ?? 'mongodb://localhost:27017/arque',
      maxPoolSize: opts?.maxPoolSize ?? 100,
      minPoolSize: opts?.minPoolSize ?? 10,
      socketTimeoutMS: opts?.socketTimeoutMS ?? 45000,
      serverSelectionTimeoutMS: opts?.serverSelectionTimeoutMS ?? 25000,
      cacheMax: opts?.cacheMax ?? 1000,
      cacheTTL: opts?.cacheTTL ?? 1000 * 60 * 60,
    };

    this.cache = new LRUCache({
      max: this.opts.cacheMax,
      ttl: this.opts.cacheTTL,
    });
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
          writeConcern: {
            w: 'majority',
          },
          readPreference: 'secondaryPreferred',
          minPoolSize: this.opts.minPoolSize,
          maxPoolSize: this.opts.maxPoolSize,
          socketTimeoutMS: this.opts?.socketTimeoutMS,
          serverSelectionTimeoutMS: this.opts?.serverSelectionTimeoutMS,
        }).asPromise();

        return connection;
      })().catch((err) => {
        delete this._connection;

        throw err;
      });
    }

    return this._connection;
  }

  private async model(model: keyof typeof schema) {
    const connection = await this.connection();

    return connection.model(model, schema[model]);
  }

  async saveStream(params: { id: string; events: number[] }): Promise<void> {
    const StreamModel = await this.model('Stream');

    await StreamModel.updateOne({
      _id: params.id,
    }, {
      $set: {
        events: params.events,
        timestamp: new Date(),
      },
    }, {
      upsert: true,
    });
  }
  
  async findStreams(event: number): Promise<string[]> {
    let streams = this.cache.get(event);

    if (streams) {
      return streams;
    }

    const StreamModel = await this.model('Stream');

    const docs = await StreamModel.find({
      events: event,
    });

    if (!docs) {
      return [];
    }

    streams = docs.map((doc) => doc._id);

    this.cache.set(event, streams);

    return streams;
  }

  async close(): Promise<void> {
    if (this._connection) {
      const connection = await this._connection;

      await connection.close();
    }
  }

}
