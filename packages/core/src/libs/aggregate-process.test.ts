import { randomBytes } from 'crypto';
import { Event, EventHandler, Command, CommandHandler } from './types';
import { Aggregate } from './aggregate';
import { arrayToAsyncIterableIterator } from './util';
import { EventId } from './event-id';
import R from 'ramda';
import { faker } from '@faker-js/faker';
import { AggregateVersionConflictError } from './adapters/store-adapter';

enum EventType {
  BalanceUpdated = 0,
}

enum CommandType {
  UpdateBalance = 0,
}

type BalanceUpdatedEvent = Event<
  EventType.BalanceUpdated,
  { balance: number; amount: number }
>;

type UpdateBalanceCommand = Command<
  CommandType.UpdateBalance,
  [{ amount: number }]
>;

type BalanceAggregateState = { balance: number };

const UpdateBalanceCommandHandler: CommandHandler<UpdateBalanceCommand, BalanceUpdatedEvent, BalanceAggregateState> = {
  type: CommandType.UpdateBalance,
  handle(ctx, _, { amount }) {
    const balance = ctx.state.balance + amount;

    if (balance < 0) {
      throw new Error('insufficient balance');
    }

    return {
      type: EventType.BalanceUpdated,
      body: { balance, amount: amount },
    };
  },
};

const BalanceUpdatedEventHandler: EventHandler<BalanceUpdatedEvent, BalanceAggregateState> = {
  type: EventType.BalanceUpdated,
  handle(_, event) {
    return {
      balance: event.body.balance,
    };
  },
};


describe('Aggregate#process', () => {
  test.concurrent('process', async () => {
    const id = randomBytes(13);

    const store = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator([])),
      findLatestSnapshot: jest.fn().mockResolvedValue(null),
      saveEvents: jest.fn().mockResolvedValue(undefined),
    };

    const stream = {
      sendEvents: jest.fn().mockResolvedValue(undefined),
    };

    const aggregate = new Aggregate<BalanceAggregateState, typeof UpdateBalanceCommandHandler, typeof BalanceUpdatedEventHandler>(
      store as never,
      stream as never,
      [UpdateBalanceCommandHandler],
      [BalanceUpdatedEventHandler],
      id,
      0,
      { balance: 0 },
    );

    await aggregate.process({
      type: CommandType.UpdateBalance,
      args: [{ amount: 10 }],
    });
    
    expect(aggregate.state).toEqual({ balance: 10 });
    expect(aggregate.version).toEqual(1);
    expect(store.listEvents).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(store.findLatestSnapshot).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(store.saveEvents).toBeCalledWith(expect.objectContaining({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: expect.any(Date),
      events: [
        expect.objectContaining({
          id: expect.any(EventId),
          type: EventType.BalanceUpdated,
          body: { balance: 10, amount: 10 },
          meta: {},
        }),
      ],
    }));
  });

  test.concurrent('invalid command', async () => {
    const id = randomBytes(13);

    const store = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator([])),
      findLatestSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate<BalanceAggregateState, typeof UpdateBalanceCommandHandler, typeof BalanceUpdatedEventHandler>(
      store as never,
      {} as never,
      [UpdateBalanceCommandHandler],
      [BalanceUpdatedEventHandler],
      id,
      0,
      { balance: 0 },
    );

    await expect(aggregate.process({
      type: CommandType.UpdateBalance,
      args: [{ amount: -10 }],
    })).rejects.toThrowError('insufficient balance');
    
    expect(aggregate.state).toEqual({ balance: 0 });
    expect(aggregate.version).toEqual(0);
    expect(store.listEvents).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(store.findLatestSnapshot).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
  });

  test.concurrent('multiple commands in succession', async () => {
    const id = randomBytes(13);

    const values = R.times(() => faker.number.float({ min: 10, max: 100, precision: 2 }), 10);

    const store = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator([])),
      findLatestSnapshot: jest.fn().mockResolvedValue(null),
      saveEvents: jest.fn().mockResolvedValue(undefined),
    };

    const stream = {
      sendEvents: jest.fn().mockResolvedValue(undefined),
    };

    const aggregate = new Aggregate<BalanceAggregateState, typeof UpdateBalanceCommandHandler, typeof BalanceUpdatedEventHandler>(
      store as never,
      stream as never,
      [UpdateBalanceCommandHandler],
      [BalanceUpdatedEventHandler],
      id,
      0,
      { balance: 0 },
    );

    for (const amount of values) {
      await aggregate.process({
        type: CommandType.UpdateBalance,
        args: [{ amount }],
      });
    }
    
    expect(aggregate.state).toEqual({ balance: R.sum(values) });
    expect(aggregate.version).toEqual(values.length);
    expect(store.listEvents).toBeCalledTimes(values.length);
    expect(store.findLatestSnapshot).toBeCalledTimes(values.length);
    expect(store.saveEvents).toBeCalledTimes(values.length);
    expect(stream.sendEvents).toBeCalledTimes(values.length);
  });

  test.concurrent('aggregate version conflict', async () => {
    const id = randomBytes(13);

    const store = {
      listEvents: jest.fn()
        .mockResolvedValue(arrayToAsyncIterableIterator([]))
        .mockResolvedValueOnce(arrayToAsyncIterableIterator([
          {
            id: new EventId(),
            type: EventType.BalanceUpdated,
            aggregate: {
              id,
              version: 5,
            },
            body: { balance: 105, amount: 5 },
            timestamp: new Date(),
          },
        ])),
      findLatestSnapshot: jest.fn().mockResolvedValue(null),
      saveEvents: jest.fn()
        .mockImplementationOnce(() => Promise.reject(new AggregateVersionConflictError(id, 5)))
        .mockResolvedValueOnce(undefined),
    };

    const stream = {
      sendEvents: jest.fn().mockResolvedValue(undefined),
    };

    const aggregate = new Aggregate<BalanceAggregateState, typeof UpdateBalanceCommandHandler, typeof BalanceUpdatedEventHandler>(
      store as never,
      stream as never,
      [UpdateBalanceCommandHandler],
      [BalanceUpdatedEventHandler],
      id,
      4,
      { balance: 100 },
    );

    await aggregate.process({
      type: CommandType.UpdateBalance,
      args: [{ amount: 10 }],
    });

    expect(aggregate.state).toEqual({ balance: 115 });
    expect(aggregate.version).toEqual(6);
    expect(store.listEvents).toBeCalledTimes(2);
    expect(store.findLatestSnapshot).toBeCalledTimes(2);
    expect(store.saveEvents).toBeCalledTimes(2);
    expect(stream.sendEvents).toBeCalledTimes(1);
  });

  test.concurrent('snapshot', async () => {
    const id = randomBytes(13);

    const store = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator([])),
      findLatestSnapshot: jest.fn().mockResolvedValue(null),
      saveEvents: jest.fn().mockResolvedValue(undefined),
      saveSnapshot: jest.fn().mockResolvedValue(undefined),
    };

    const stream = {
      sendEvents: jest.fn().mockResolvedValue(undefined),
    };

    const aggregate = new Aggregate<BalanceAggregateState, typeof UpdateBalanceCommandHandler, typeof BalanceUpdatedEventHandler>(
      store as never,
      stream as never,
      [UpdateBalanceCommandHandler],
      [BalanceUpdatedEventHandler],
      id,
      0,
      { balance: 0 },
      { snapshotInterval: 10 },
    );

    const count = 45;

    for (const index of R.range(0, count)) {
      await aggregate.process({
        type: CommandType.UpdateBalance,
        args: [{ amount: index % 2 === 0 ? 10 : -5 }],
      });
    }
    
    expect(aggregate.state).toEqual({ balance: 10 * Math.ceil(count / 2) - 5 * Math.floor(count / 2) });
    expect(aggregate.version).toEqual(count);
    expect(store.listEvents).toBeCalledTimes(count);
    expect(store.findLatestSnapshot).toBeCalledTimes(count);
    expect(store.saveEvents).toBeCalledTimes(count);
    expect(stream.sendEvents).toBeCalledTimes(count);
    expect(store.saveSnapshot).toBeCalledTimes(Math.floor(count / 10));
  });
});
