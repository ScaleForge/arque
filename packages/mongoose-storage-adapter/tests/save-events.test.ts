import { generateFakeEvent } from './helpers/generate-fake-event';
import R from 'ramda';
import { randomBytes } from 'crypto';
import { AggregateVersionConflictError } from '@arque/core';
import { setupFixture } from './helpers/fixture';

describe('MongooseEventStoreStorageAdapter#saveEvents', () => {
  test.concurrent('save event', async () => {
    const event = generateFakeEvent();

    const { adapter, teardown } = await setupFixture();

    await adapter.saveEvents({
      aggregate: event.aggregate,
      timestamp: event.timestamp,
      events: [R.pick(['id', 'type', 'body', 'meta'], event)],
    });

    await teardown();
  });

  test.concurrent('save multiple events', async () => {
    const event = generateFakeEvent();

    const { adapter, teardown } = await setupFixture();

    await adapter.saveEvents({
      aggregate: event.aggregate,
      timestamp: event.timestamp,
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent()), 100),
    });

    await teardown();
  });

  test.concurrent('save consecutive events', async () => {
    const event = generateFakeEvent();

    const { adapter, teardown } = await setupFixture();

    for await (const version of R.range(1, 101)) {
      await adapter.saveEvents({
        aggregate: {
          ...event.aggregate,
          version,
        },
        timestamp: event.timestamp,
        events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
      });
    }

    await teardown();
  });

  test.concurrent('save events concurrently', async () => {
    const { adapter, teardown } = await setupFixture();

    const results = await Promise.allSettled(R.times(async () => {
      const event = generateFakeEvent();

      for await (const version of R.range(1, 101)) {
        await adapter.saveEvents({
          aggregate: {
            ...event.aggregate,
            version,
          },
          timestamp: event.timestamp,
          events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
        });
      }
    }, 10));

    await teardown();

    expect(R.find((item) => item.status === 'rejected', results)).toBeUndefined();
  });

  test.concurrent('aggregate version conflict', async () => {
    const aggregate = randomBytes(13);

    const { adapter, teardown } = await setupFixture();

    await adapter.saveEvents({
      aggregate: {
        id: aggregate,
        version: 1,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
    });

    await expect(adapter.saveEvents({
      aggregate: {
        id: aggregate,
        version: 1,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
    })).rejects.toThrow(AggregateVersionConflictError);

    await teardown();
  });

  test.concurrent('aggregate version conflict', async () => {
    const aggregate = randomBytes(13);

    const { adapter, teardown } = await setupFixture();

    await adapter.saveEvents({
      aggregate: {
        id: aggregate,
        version: 1,
      },
      timestamp: new Date(),
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent()), 5),
    });

    await expect(adapter.saveEvents({
      aggregate: {
        id: aggregate,
        version: 7,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
    })).rejects.toThrow(AggregateVersionConflictError);

    await expect(adapter.saveEvents({
      aggregate: {
        id: aggregate,
        version: 5,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
    })).rejects.toThrow(AggregateVersionConflictError);

    await expect(adapter.saveEvents({
      aggregate: {
        id: aggregate,
        version: 6,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent())],
    })).resolves.toBeUndefined();

    await teardown();
  });
});