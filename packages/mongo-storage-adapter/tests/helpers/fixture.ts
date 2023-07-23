import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongoStorageAdapter } from '../../src';

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

  const adapter = new MongoStorageAdapter({
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