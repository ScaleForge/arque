import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongooseEventStoreStorageAdapter } from '../src';
import { generateFakeEvent } from './helpers';
import R from 'ramda';

async function setupFixture() {
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

describe('MongooseEventStoreStorageAdapter#saveEvents', () => {
  test.concurrent('save event', async () => {
    console.time();
    const event = generateFakeEvent();

    const { adapter, teardown } = await setupFixture();
    console.timeLog();
    const txn = await adapter.saveEvents({
      aggregate: event.aggregate,
      timestamp: event.timestamp,
      events: [R.pick(['id', 'type', 'body', 'meta'], event)],
    });
    console.timeLog();
    await txn.commit();
    console.timeLog();
    await teardown();
    console.timeLog();
  });

  test.concurrent('save multiple events', async () => {
    const event = generateFakeEvent();

    const { adapter, teardown } = await setupFixture();

    const txn = await adapter.saveEvents({
      aggregate: event.aggregate,
      timestamp: event.timestamp,
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent()), 100),
    });

    await txn.commit();

    await teardown();
  });

  test.concurrent('save consecutive events', async () => {
    const event = generateFakeEvent();

    const { adapter, teardown } = await setupFixture();

    for await (const version of R.range(1, 101)) {
      const txn = await adapter.saveEvents({
        aggregate: {
          ...event.aggregate,
          version,
        },
        timestamp: event.timestamp,
        events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
      });

      await txn.commit();
    }

    await teardown();
  });
});