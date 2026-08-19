import { randomBytes } from 'crypto';
import { backOff } from 'exponential-backoff';
import { Connection, Model } from 'mongoose';
import { LockSchema, LockDocument } from './schema';

const LOCK_POLL_STARTING_DELAY_MS = 32;
const LOCK_POLL_MAX_DELAY_MS = 512;
const LOCK_LEASE_MS = 30_000;

class LockHeldError extends Error {
  constructor() {
    super('lock is already held');
  }
}

export class Lock {
  private active = true;

  private constructor(
    private readonly model: Model<LockDocument>,
    private readonly key: Buffer,
    private readonly owner: Buffer,
  ) {}

  async extend(): Promise<void> {
    if (!this.active) {
      throw new Error('lock is no longer active');
    }

    const timestamp = new Date();

    const result = await this.model.updateOne({
      _id: this.key,
      owner: this.owner,
      timestamp: { $gt: new Date(timestamp.getTime() - LOCK_LEASE_MS) },
    }, {
      $set: {
        timestamp: timestamp,
      },
    }, {
      readPreference: 'primary',
      writeConcern: {
        w: 'majority',
      },
    });

    if (result.matchedCount !== 1) {
      this.active = false;

      throw new Error('lock has expired or ownership was lost');
    }
  }

  async release(): Promise<void> {
    if (!this.active) {
      return;
    }

    try {
      await this.model.deleteOne({
        _id: this.key,
        owner: this.owner,
      }, {
        readPreference: 'primary',
        writeConcern: {
          w: 'majority',
        },
      });
    } finally {
      this.active = false;
    }
  }

  static async acquire(connection: Connection, key: Buffer): Promise<Lock> {
    const model = <Model<LockDocument>>connection.models.Lock ?? connection.model<LockDocument>('Lock', LockSchema);

    const _id = Buffer.from(key);

    const owner = randomBytes(16);

    return backOff(async () => {
      const existing = await model.findOne({ _id })
        .select({ _id: 1 })
        .read('primary');

      if (existing) {
        throw new LockHeldError();
      }

      await model.insertOne({
        _id,
        owner,
        timestamp: new Date(),
      }, {
        w: 'majority',
      });

      return new Lock(model, _id, owner);
    }, {
      startingDelay: LOCK_POLL_STARTING_DELAY_MS,
      maxDelay: LOCK_POLL_MAX_DELAY_MS,
      numOfAttempts: 32,
      jitter: 'full',
      retry: (error) => {
        const err = error as { code?: number; codeName?: string };

        return error instanceof LockHeldError || err.code === 11000 || err.codeName === 'DuplicateKey';
      },
    });
  }
}
