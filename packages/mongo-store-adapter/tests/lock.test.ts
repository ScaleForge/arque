import { randomBytes } from 'crypto';
import { Lock } from '../src/libs/lock';
import { setupFixture } from './helpers/fixture';

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('Lock', () => {
  test.concurrent('blocks the same key and allows different keys', async () => {
    const { connection, teardown } = await setupFixture();
    const key = randomBytes(13);
    const otherKey = randomBytes(13);

    const lock = await Lock.acquire(connection, key);
    const findOne = jest.spyOn(connection.models.Lock, 'findOne');
    const waiting = Lock.acquire(connection, key);
    const different = await Lock.acquire(connection, otherKey);

    await expect(Promise.race([
      waiting.then(() => true),
      wait(250).then(() => false),
    ])).resolves.toBe(false);
    expect(findOne).not.toHaveBeenCalled();

    await different.release();
    await lock.release();
    await expect(waiting).resolves.toBeDefined();
    await waiting.then((item) => item.release());

    await teardown();
  });

  test.concurrent('configures and extends a thirty-second lease', async () => {
    const { connection, teardown } = await setupFixture();
    const key = randomBytes(13);
    const startedAt = Date.now();
    const lock = await Lock.acquire(connection, key);
    const acquiredAt = Date.now();
    const collection = connection.collection('locks');

    const document = await collection.findOne({ _id: key }, {
      readPreference: 'primary',
    });

    const timestamp = document?.['timestamp'] as Date;

    expect(timestamp.getTime()).toBeGreaterThanOrEqual(startedAt);
    expect(timestamp.getTime()).toBeLessThanOrEqual(acquiredAt);

    await wait(10);
    const beforeExtend = Date.now();
    await lock.extend();
    const afterExtend = Date.now();

    const extended = await collection.findOne({ _id: key }, {
      readPreference: 'primary',
    });

    expect((extended?.['timestamp'] as Date).getTime()).toBeGreaterThanOrEqual(beforeExtend);
    expect((extended?.['timestamp'] as Date).getTime()).toBeLessThanOrEqual(afterExtend);

    await lock.release();
    await expect(collection.findOne({ _id: key })).resolves.toBeNull();
    await teardown();
  });

  test.concurrent('does not allow a stale owner to release a replacement', async () => {
    const { connection, teardown } = await setupFixture();
    const key = randomBytes(13);
    const collection = connection.collection('locks');
    const stale = await Lock.acquire(connection, key);

    await collection.deleteOne({ _id: key }, {
      writeConcern: { w: 'majority' },
    });

    const replacement = await Lock.acquire(connection, key);
    await stale.release();

    await expect(collection.findOne({ _id: key })).resolves.not.toBeNull();

    await replacement.release();
    await teardown();
  });

  test.concurrent('does not allow a stale owner to extend a replacement', async () => {
    const { connection, teardown } = await setupFixture();
    const key = randomBytes(13);
    const collection = connection.collection('locks');
    const stale = await Lock.acquire(connection, key);

    await collection.deleteOne({ _id: key }, {
      writeConcern: { w: 'majority' },
    });

    const replacement = await Lock.acquire(connection, key);
    const before = await collection.findOne({ _id: key }, {
      readPreference: 'primary',
    });

    await expect(stale.extend()).rejects.toThrow('lock has expired or ownership was lost');

    const after = await collection.findOne({ _id: key }, {
      readPreference: 'primary',
    });

    expect(after?.['owner']).toEqual(before?.['owner']);
    expect(after?.['timestamp']).toEqual(before?.['timestamp']);

    await replacement.release();
    await teardown();
  });
});
