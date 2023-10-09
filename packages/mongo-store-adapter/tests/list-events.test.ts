import R from 'ramda';
import { randomBytes } from 'crypto';
import { EventId } from '@arque/core';
import { setupFixture } from './helpers/fixture';
import { generateEvent } from './helpers/generate-event';

describe('MongoStorageAdapter#listEvents', () => {
  test.concurrent('list events', async () => {
    const id = randomBytes(13);

    const { store, teardown } = await setupFixture();

    await store.saveEvents({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: new Date(),
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateEvent()), 100),
    });

    const events = await store.listEvents({
      aggregate: {
        id,
      },
    });

    let version = 1;
    for await (const event of events) {
      expect(event).toMatchObject({
        id: expect.any(EventId),
        type: expect.any(Number),
        aggregate: {
          id,
          version: version++,
        },
        body: expect.any(Object),
        meta: expect.any(Object),
        timestamp: expect.any(Date),
      });
    }

    await teardown();
  });

  test.concurrent('list events', async () => {
    const id = randomBytes(13);

    const { store, teardown } = await setupFixture();

    await Promise.all([
      store.saveEvents({
        aggregate: {
          id,
          version: 1,
        },
        timestamp: new Date(),
        events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateEvent()), 100),
      }),
      store.saveEvents({
        aggregate: {
          id: randomBytes(13),
          version: 1,
        },
        timestamp: new Date(),
        events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateEvent()), 100),
      }),
    ]);

    const events = await store.listEvents({
      aggregate: {
        id,
        version: 5,
      },
    });

    let version = 6;
    for await (const event of events) {
      expect(event).toMatchObject({
        id: expect.any(EventId),
        type: expect.any(Number),
        aggregate: {
          id,
          version: version++,
        },
        body: expect.any(Object),
        meta: expect.any(Object),
        timestamp: expect.any(Date),
      });
    }

    await teardown();
  });
});