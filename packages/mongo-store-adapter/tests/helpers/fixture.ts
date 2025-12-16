import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongoStoreAdapter } from '../../src';
import mongoose from 'mongoose';

export async function setupFixture() {
  const mongo = await MongoMemoryReplSet.create({
    binary: {
      version: '8.0.3',
    },
    replSet: {
      storageEngine: 'wiredTiger',
      count: 3,
    },
  });

  await mongo.waitUntilRunning();

  const store = new MongoStoreAdapter({
    uri: mongo.getUri(),
  });

  const connection = await mongoose.createConnection(mongo.getUri()).asPromise();
  
  return {
    store,
    connection,
    async teardown() {
      await store.close();
      await connection.close();
      await mongo.stop();
    },
  };
}
