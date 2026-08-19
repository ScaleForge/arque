import { randomBytes } from 'crypto';
import { Connection } from 'mongoose';
import { Lock } from './lock';

let mockRetryError: unknown;
let mockShouldRetry: boolean | Promise<boolean>;

jest.mock('exponential-backoff', () => ({
  backOff: async (
    request: () => Promise<unknown>,
    options: { retry: (error: unknown, attemptNumber: number) => boolean | Promise<boolean> },
  ) => {
    try {
      return await request();
    } catch (error) {
      mockRetryError = error;
      mockShouldRetry = options.retry(error, 1);

      throw error;
    }
  },
}));

describe('Lock.acquire', () => {
  test('translates duplicate inserts without reading the lock first', async () => {
    const findOne = jest.fn();
    const insertOne = jest.fn().mockRejectedValue(Object.assign(new Error('duplicate key'), {
      code: 11000,
    }));
    const connection = {
      models: {
        Lock: { findOne, insertOne },
      },
    } as unknown as Connection;

    await expect(Lock.acquire(connection, randomBytes(13))).rejects.toThrow('lock is already held');

    expect(findOne).not.toHaveBeenCalled();
    expect((mockRetryError as Error).constructor.name).toBe('LockHeldError');
    expect(mockShouldRetry).toBe(true);
  });
});
