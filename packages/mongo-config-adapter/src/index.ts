import { ConfigAdapter } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import * as schema from './libs/schema';
import debug from 'debug';
import { LRUCache } from 'lru-cache';

type Options = {
  readonly uri: string;
  readonly cacheSize: number;
} & Readonly<Pick<ConnectOptions, 'maxPoolSize' | 'minPoolSize' | 'socketTimeoutMS' | 'serverSelectionTimeoutMS'>>;

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
      cacheSize: opts?.cacheSize ?? 10000,
    };

    this.cache = new LRUCache({
      maxSize: this.opts.cacheSize,
      sizeCalculation: (value) => {
        return value.length;
      },
      ttl: 1000 * 60 * 60,
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
