import { randomBytes } from 'crypto';
import { Lock } from '../src/libs/lock';
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

  test.concurrent('deletes snapshots older than the previous version', async () => {
    const aggregateId = randomBytes(13);
    const otherAggregateId = randomBytes(13);

    const { store, teardown } = await setupFixture();

    for (const version of [1, 2]) {
      await store.saveSnapshot({
        aggregate: {
          id: otherAggregateId,
          version,
        },
        timestamp: new Date(),
        state: { version },
      });
    }

    for (const version of [1, 3, 5]) {
      await store.saveSnapshot({
        aggregate: {
          id: aggregateId,
          version,
        },
        timestamp: new Date(),
        state: { version },
      });
    }

    const SnapshotModel = await store.model('Snapshot');
    const snapshots = await SnapshotModel.find({
      'aggregate.id': aggregateId,
    }).sort({
      'aggregate.version': 1,
    }).lean();
    const otherSnapshots = await SnapshotModel.find({
      'aggregate.id': otherAggregateId,
    }).sort({
      'aggregate.version': 1,
    }).lean();

    expect(snapshots.map((snapshot) => snapshot['aggregate']['version'])).toEqual([5]);
    expect(otherSnapshots.map((snapshot) => snapshot['aggregate']['version'])).toEqual([1, 2]);

    await teardown();
  });

  test.concurrent('locks snapshots by aggregate', async () => {
    const aggregateId = randomBytes(13);
    const otherAggregateId = randomBytes(13);
    const { store, connection, teardown } = await setupFixture();
    const lock = await Lock.acquire(connection, aggregateId);
    let completed = false;

    const waiting = store.saveSnapshot({
      aggregate: {
        id: aggregateId,
        version: 1,
      },
      timestamp: new Date(),
      state: { aggregate: 'locked' },
    }).then(() => {
      completed = true;
    });

    await new Promise((resolve) => setImmediate(resolve));
    expect(completed).toBe(false);

    await store.saveSnapshot({
      aggregate: {
        id: otherAggregateId,
        version: 1,
      },
      timestamp: new Date(),
      state: { aggregate: 'other' },
    });
    expect(completed).toBe(false);

    await lock.release();
    await waiting;
    await teardown();
  });
});
