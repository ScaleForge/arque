import { randomBytes } from 'crypto';
import { backOff } from 'exponential-backoff';
import { Collection, Connection, Model } from 'mongoose';
import { LockSchema, LockDocument } from './schema';

const LOCK_POLL_STARTING_DELAY_MS = 32;
const LOCK_POLL_MAX_DELAY_MS = 512;
const LOCK_LEASE_MS = 30_000;
const lockHeld = new Error('lock is already held');

type LockCollection = Collection<LockDocument>;
type LockModel = Model<LockDocument>;

const collections = new WeakMap<Connection, Promise<LockCollection>>();

function isDuplicateKeyError(error: unknown) {
  const err = error as { code?: number; codeName?: string };

  return err.code === 11000 || err.codeName === 'DuplicateKey';
}

function collectionFor(connection: Connection) {
  const existing = collections.get(connection);

  if (existing) {
    return existing;
  }

  const initialization = (async () => {
    const model = <LockModel>(connection.models.Lock ?? connection.model<LockDocument>('Lock', LockSchema));

    try {
      await model.init();
    } catch (error) {
      if (connection.models.Lock === model) {
        connection.deleteModel('Lock');
      }

      throw error;
    }

    return <LockCollection><unknown>model.collection;
  })();

  let cached: Promise<LockCollection>;

  cached = initialization.catch((error) => {
    if (collections.get(connection) === cached) {
      collections.delete(connection);
    }

    throw error;
  });

  collections.set(connection, cached);

  return cached;
}

export class Lock {
  private active = true;

  private constructor(
    private readonly collection: LockCollection,
    private readonly key: Buffer,
    private readonly owner: Buffer,
  ) {}

  async extend(): Promise<void> {
    if (!this.active) {
      throw new Error('lock is no longer active');
    }

    const timestamp = new Date();

    const result = await this.collection.updateOne({
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
      await this.collection.deleteOne({
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
    const collection = await collectionFor(connection);
    const lockKey = Buffer.from(key);
    const owner = randomBytes(16);

    return backOff(async () => {
      const existing = await collection.findOne({
        _id: lockKey,
      }, {
        projection: {
          _id: 1,
        },
        readPreference: 'primary',
      });

      if (existing) {
        throw lockHeld;
      }

      await collection.insertOne({
        _id: lockKey,
        owner,
        timestamp: new Date(),
      }, {
        readPreference: 'primary',
        writeConcern: {
          w: 'majority',
        },
      });

      return new Lock(collection, lockKey, owner);
    }, {
      startingDelay: LOCK_POLL_STARTING_DELAY_MS,
      maxDelay: LOCK_POLL_MAX_DELAY_MS,
      numOfAttempts: 32,
      jitter: 'full',
      retry: (error) => error === lockHeld || isDuplicateKeyError(error),
    });
  }
}
