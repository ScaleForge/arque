import R from 'ramda';
import { randomBytes } from 'crypto';
import { EventId } from '@arque/core';
import { setupFixture } from './helpers/fixture';
import { generateFakeEvent } from './helpers/generate-fake-event';

describe('MongooseEventStoreStorageAdapter#listEvents', () => {
  test.concurrent('list events', async () => {
    const aggregate = randomBytes(13);

    const { adapter, teardown } = await setupFixture();

    await adapter.saveEvents({
      aggregate: {
        id: aggregate,
        version: 1,
      },
      timestamp: new Date(),
      events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent()), 100),
    });

    const events = await adapter.listEvents({
      aggregate: {
        id: aggregate,
      },
    });

    let version = 1;
    for await (const event of events) {
      expect(event).toMatchObject({
        id: expect.any(EventId),
        type: expect.any(Number),
        aggregate: {
          id: aggregate,
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
    const aggregate = randomBytes(13);

    const { adapter, teardown } = await setupFixture();

    await Promise.all([
      adapter.saveEvents({
        aggregate: {
          id: aggregate,
          version: 1,
        },
        timestamp: new Date(),
        events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent()), 100),
      }),
      adapter.saveEvents({
        aggregate: {
          id: randomBytes(13),
          version: 1,
        },
        timestamp: new Date(),
        events: R.times(() => R.pick(['id', 'type', 'body', 'meta'], generateFakeEvent()), 100),
      }),
    ]);

    const events = await adapter.listEvents({
      aggregate: {
        id: aggregate,
        version: 5,
      },
    });

    let version = 6;
    for await (const event of events) {
      expect(event).toMatchObject({
        id: expect.any(EventId),
        type: expect.any(Number),
        aggregate: {
          id: aggregate,
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