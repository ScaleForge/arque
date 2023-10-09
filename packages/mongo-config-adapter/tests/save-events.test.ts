import { generateEvent } from './helpers/generate-event';
import R from 'ramda';
import { setupFixture } from './helpers/fixture';
import { randomBytes } from 'crypto';
import { AggregateVersionConflictError } from '@arque/core';

describe('MongooseEventStoreStorageAdapter#saveEvents', () => {
  test.concurrent('save event', async () => {
    const event = generateEvent();

    const { store, connection, teardown } = await setupFixture();

    await store.saveEvents({
      aggregate: event.aggregate,
      timestamp: event.timestamp,
      events: [R.pick(['id', 'type', 'body', 'meta'], event)],
    });
    const cursor = await connection.collection('events').find({});
    console.dir(await cursor.tryNext(), { depth: 5 });

    await teardown();
  });

  test.concurrent('save multiple events', async () => {
    const { store, teardown } = await setupFixture();

    await store.saveEvents({
      aggregate: {
        id: randomBytes(13),
        version: 1,
      },
      timestamp: new Date(),
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateEvent()), 100),
    });

    await teardown();
  });

  test.concurrent('save consecutive events', async () => {
    const id = randomBytes(13);

    const { store, teardown } = await setupFixture();

    for await (const version of R.range(1, 101)) {
      await store.saveEvents({
        aggregate: {
          id,
          version,
        },
        timestamp: new Date(),
        events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
      });
    }

    await teardown();
  });

  test.concurrent('save events concurrently', async () => {
    const { store, teardown } = await setupFixture();

    const results = await Promise.allSettled(R.times(async () => {
      const event = generateEvent();

      for await (const version of R.range(1, 101)) {
        await store.saveEvents({
          aggregate: {
            ...event.aggregate,
            version,
          },
          timestamp: event.timestamp,
          events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
        });
      }
    }, 10));

    await teardown();

    expect(R.find((item) => item.status === 'rejected', results)).toBeUndefined();
  });

  test.concurrent('aggregate version conflict', async () => {
    const id = randomBytes(13);

    const { store, teardown } = await setupFixture();

    await store.saveEvents({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    });

    await expect(store.saveEvents({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    })).rejects.toThrow(AggregateVersionConflictError);

    await teardown();
  });

  test.concurrent('aggregate version conflict', async () => {
    const id = randomBytes(13);

    const { store, teardown } = await setupFixture();

    await store.saveEvents({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: new Date(),
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateEvent()), 5),
    });

    await expect(store.saveEvents({
      aggregate: {
        id,
        version: 7,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    })).rejects.toThrow(AggregateVersionConflictError);

    await expect(store.saveEvents({
      aggregate: {
        id,
        version: 5,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    })).rejects.toThrow(AggregateVersionConflictError);

    await expect(store.saveEvents({
      aggregate: {
        id,
        version: 6,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    })).resolves.toBeUndefined();

    await teardown();
  });
});