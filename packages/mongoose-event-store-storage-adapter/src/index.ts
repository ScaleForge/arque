import { Connection } from 'mongoose';

export class MongooseEventStoreStorageAdapter {
  private connectionPromise: Promise<Connection>;

  constructor(private readonly opts?: {
    readonly connection?: string;
  }) {
  }
}