import { randomBytes } from 'crypto';
import R from 'ramda';
import { EventId } from './event-id';
import { Event, EventHandler } from './types';
import { arrayToAsyncIterableIterator } from './util';
import { Aggregate } from './aggregate';

enum EventType {
  BalanceUpdated = 0 || 1 << 8,
}

type BalanceAggregateState = { balance: number };

type BalanceUpdatedEvent = Event<
  EventType.BalanceUpdated,
  { balance: number; amount: number }
>;

const BalanceUpdatedEventHandler: EventHandler<BalanceUpdatedEvent, BalanceAggregateState> = {
  type: EventType.BalanceUpdated,
  handle(_, event: BalanceUpdatedEvent) {
    return {
      balance: event.body.balance,
    };
  },
};

describe('Aggregate#reload', () => {
  test.concurrent('reload', async () => {
    const id = randomBytes(13);
    const amount = 10;
    const timestamp = new Date();

    const events = R.times((index) => ({
      id: EventId.generate(),
      type: EventType.BalanceUpdated,
      aggregate: {
        id,
        version: index + 1,
      },
      body: { balance: (index + 1) * amount, amount },
      timestamp,
      meta: {},
    }), 10);

    const store = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator(events)),
      findLatestSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate(
      store as never,
      {} as never,
      [],
      [BalanceUpdatedEventHandler],
      id,
      0,
      { balance: 0 },
    );

    await aggregate.reload();

    expect(aggregate.state).toEqual({ balance: events.length * amount });
    expect(aggregate.version).toEqual(events.length);
    expect(store.listEvents).toHaveBeenCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(store.findLatestSnapshot).toHaveBeenCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
  });

  test.concurrent('snapshot', async () => {
    const id = randomBytes(13);
    const amount = 10;
    const timestamp = new Date();

    const events = R.times((index) => ({
      id: EventId.generate(),
      type: EventType.BalanceUpdated,
      aggregate: {
        id,
        version: index + 6,
      },
      body: { balance: (index + 6) * amount, amount },
      timestamp,
      meta: {},
    }), 5);

    const store = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator(events)),
      findLatestSnapshot: jest.fn().mockResolvedValue({
        aggregate: {
          id,
          version: 5,
        },
        state: {
          balance: 50,
        },
        timestamp,
      }),
    };

    const aggregate = new Aggregate(
      store as never,
      {} as never,
      [],
      [BalanceUpdatedEventHandler],
      id,
      0,
      { balance: 0 },
    );

    await aggregate.reload();

    expect(aggregate.state).toEqual({ balance: 50 + events.length * amount });
    expect(aggregate.version).toEqual(events.length + 5);
    expect(store.listEvents).toHaveBeenCalledWith({
      aggregate: {
        id,
        version: 5,
      },
    });
    expect(store.findLatestSnapshot).toHaveBeenCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
  });

  test.concurrent('multiple concurrent reloads', async () => {
    const id = randomBytes(13);
    const amount = 10;
    const timestamp = new Date();

    const events = R.times((index) => ({
      id: EventId.generate(),
      type: EventType.BalanceUpdated,
      aggregate: {
        id,
        version: index + 1,
      },
      body: { balance: (index + 1) * amount, amount },
      timestamp,
      meta: {},
    }), 15);

    const store = {
      listEvents: jest.fn(async ({ aggregate: { version } }) => arrayToAsyncIterableIterator(events.slice(version))),
      findLatestSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate(
      store as never,
      {} as never,
      [],
      [BalanceUpdatedEventHandler],
      id,
      0,
      { balance: 0 },
    );

    await Promise.all([
      aggregate.reload(),
      aggregate.reload(),
      aggregate.reload(),
      aggregate.reload(),
      aggregate.reload(),
    ]);

    expect(aggregate.state).toEqual({ balance: events.length * amount });
    expect(aggregate.version).toEqual(events.length);
    expect(store.listEvents).toHaveBeenCalledTimes(5);
    expect(store.findLatestSnapshot).toHaveBeenCalledTimes(5);
  });
});