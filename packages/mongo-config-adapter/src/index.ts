import { ConfigAdapter } from '@arque/core';
import mongoose, { Connection, ConnectOptions } from 'mongoose';
import * as schema from './libs/schema';

export class MongoConfigAdapter implements ConfigAdapter {
  private connectionPromise: Promise<Connection>;

  constructor(private readonly opts?: {
    readonly uri?: string;
  } & Readonly<Pick<ConnectOptions, 'maxPoolSize' | 'minPoolSize' | 'socketTimeoutMS' | 'serverSelectionTimeoutMS'>>) {}

  private async connection() {
    if (!this.connectionPromise) {
      this.connectionPromise = mongoose.createConnection(this.opts?.uri ?? 'mongodb://localhost:27017/arque', {
        writeConcern: {
          w: 1,
        },
        readPreference: 'secondary',
        minPoolSize: this.opts?.maxPoolSize ?? 1,
        maxPoolSize: this.opts?.maxPoolSize ?? 4,
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

  async saveStream(params: {
    name: string;
    events: number[];
  }): Promise<void> {
    const StreamModel = await this.model('Stream');

    await StreamModel.updateOne({
      name: params.name,
    }, {
      $set: {
        events: params.events,
      },
    }, {
      upsert: true,
      writeConcern: {
        w: 'majority',
      },
    });
  }

  async getStream(params: { name: string }): Promise<{
    name: string;
    events: number[];
  } | null> {
    const StreamModel = await this.model('Stream');

    const stream = await StreamModel.findOne({
      name: params.name,
    });

    if (!stream) {
      return null;
    }

    return {
      name: stream['name'],
      events: stream['events'],
    };
  }

  async listStreams(params: { event: number; }): Promise<string[]> {
    const StreamModel = await this.model('Stream');

    const result = await StreamModel.find({
      events: params.event,
    }, {
      _id: 0,
      events: 1,
    });
    
    return result ? result['events'] : [];
  }

  async close(): Promise<void> {
    if (this.connectionPromise) {
      const connection = await this.connectionPromise;

      await connection.close();
    }
  }
}
