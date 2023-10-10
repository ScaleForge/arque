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
    info: debug('MongoStoreAdapter:info'),
    error: debug('MongoStoreAdapter:error'),
    warn: debug('MongoStoreAdapter:warn'),
    verbose: debug('MongoStoreAdapter:verbose'),
  };

  private readonly opts: Options;

  private readonly cache: LRUCache<number, string[]>;

  private connectionPromise: Promise<Connection>;

  constructor(opts?: Partial<Options>) {
    this.opts = {
      uri: opts?.uri ?? 'mongodb://localhost:27017/arque',
      maxPoolSize: opts?.maxPoolSize ?? 10,
      minPoolSize: opts?.minPoolSize ?? 2,
      socketTimeoutMS: opts?.socketTimeoutMS ?? 45000,
      serverSelectionTimeoutMS: opts?.serverSelectionTimeoutMS ?? 10000,
      cacheMax: opts?.cacheMax ?? 1000,
      cacheTTL: opts?.cacheTTL ?? 1000 * 60 * 60,
    };

    this.cache = new LRUCache({
      max: this.opts.cacheMax,
      ttl: this.opts.cacheTTL,
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
    if (this.connectionPromise) {
      const connection = await this.connectionPromise;

      await connection.close();
    }
  }

}
