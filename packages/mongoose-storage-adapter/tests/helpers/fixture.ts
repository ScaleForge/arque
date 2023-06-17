import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongooseEventStoreStorageAdapter } from '../../src';

export async function setupFixture() {
  const mongo = await MongoMemoryReplSet.create({
    replSet: {
      storageEngine: 'wiredTiger',
      count: 3,
    },
    instanceOpts: [
      {
        launchTimeout: 15000,
      },
    ],
  });

  await mongo.waitUntilRunning();

  const adapter = new MongooseEventStoreStorageAdapter({
    uri: mongo.getUri(),
  });

  return {
    adapter,
    async teardown() {
      await adapter.close();
      await mongo.stop();
    },
  };
}