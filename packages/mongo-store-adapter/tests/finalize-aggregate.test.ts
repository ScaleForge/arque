import R from 'ramda';
import { randomBytes } from 'crypto';
import { EventId } from '@arque/core';
import { setupFixture } from './helpers/fixture';
import { generateEvent } from './helpers/generate-event';

describe('MongoStoreAdapter#finalizeAggregate', () => {
  test.concurrent('finalize aggregate', async () => {
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

    const events = await store.finalizeAggregate({ id });

    await teardown();
  });
});