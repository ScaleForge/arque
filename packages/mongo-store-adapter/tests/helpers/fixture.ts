import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongoStoreAdapter } from '../../src';
import mongoose from 'mongoose';

export async function setupFixture() {
  const mongo = await MongoMemoryReplSet.create({
    replSet: {
      storageEngine: 'wiredTiger',
      count: 1,
    },
    instanceOpts: [
      {
        launchTimeout: 15000,
      },
    ],
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