import { generateEvent } from './helpers/generate-event';
import R from 'ramda';
import { setupFixture } from './helpers/fixture';
import { randomBytes } from 'crypto';
import { AggregateVersionConflictError, Event, StoreAdapter } from '@arque/core';
import { backOff } from 'exponential-backoff';

async function saveEvents(params: {
  aggregate: { id: Buffer; version: number; };
  timestamp: Date;
  events: Pick<Event, 'id' | 'type' | 'body' | 'meta'>[];
}, store: StoreAdapter) {
  await backOff(async () => store.saveEvents(params), {
    startingDelay: 10,
    maxDelay: 200,
    numOfAttempts: 16,
    retry: (error) => error instanceof AggregateVersionConflictError,
  });
}

describe('MongoStoreAdapter#saveEvents', () => {
  test.concurrent('save event', async () => {
    const event = generateEvent();

    const { store, teardown } = await setupFixture();

    await store.saveEvents({
      aggregate: event.aggregate,
      timestamp: event.timestamp,
      events: [R.pick(['id', 'type', 'body', 'meta'], event)],
    });

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
      await saveEvents({
        aggregate: {
          id,
          version,
        },
        timestamp: new Date(),
        events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
      }, store);
    }

    await teardown();
  });

  test.concurrent('save events concurrently', async () => {
    const { store, teardown } = await setupFixture();

    const results = await Promise.allSettled(R.times(async () => {
      const event = generateEvent();

      for await (const version of R.range(1, 101)) {
        await saveEvents({
          aggregate: {
            ...event.aggregate,
            version,
          },
          timestamp: event.timestamp,
          events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
        }, store);
      }
    }, 10));

    await teardown();

    expect(R.find((item) => item.status === 'rejected', results)).toBeUndefined();
  });

  test.concurrent('aggregate version conflict', async () => {
    const id = randomBytes(13);

    const { store, teardown } = await setupFixture();

    await saveEvents({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    }, store);

    await expect(saveEvents({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    }, store)).rejects.toThrow(AggregateVersionConflictError);

    await teardown();
  });

  test.concurrent('aggregate version conflict', async () => {
    const id = randomBytes(13);

    const { store, teardown } = await setupFixture();

    await saveEvents({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: new Date(),
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateEvent()), 5),
    }, store);

    await expect(saveEvents({
      aggregate: {
        id,
        version: 7,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    }, store)).rejects.toThrow(AggregateVersionConflictError);

    await expect(saveEvents({
      aggregate: {
        id,
        version: 5,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    }, store)).rejects.toThrow(AggregateVersionConflictError);

    await expect(saveEvents({
      aggregate: {
        id,
        version: 6,
      },
      timestamp: new Date(),
      events: [R.pick(['id', 'type', 'body', 'meta'], generateEvent())],
    }, store)).resolves.toBeUndefined();

    await teardown();
  });
});