import { setupFixture } from './helpers/fixture';
import { generateEvent } from './helpers/generate-event';

describe('MongoStoreAdapter#saveSnapshot', () => {
  test.concurrent('save event', async () => {
    const event = generateEvent();

    const { store, teardown } = await setupFixture();

    await store.saveSnapshot({
      aggregate: event.aggregate,
      timestamp: event.timestamp,
      state: {
        number: 1,
        string: 'string',
        boolean: true,
        null: null,
      },
    });

    await teardown();
  });
});