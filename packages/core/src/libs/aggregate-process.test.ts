import { randomBytes } from 'crypto';
import R from 'ramda';
import { faker } from '@faker-js/faker';
import { setTimeout } from 'timers/promises';
import { Event, EventHandler, Command, CommandHandler } from './types';
import { Aggregate } from './aggregate';
import { EventStore } from './event-store';
import { EventId } from './event-id';
import { AggregateVersionConflictError } from './error';
import { arrayToAsyncIterableIterator } from '../../__tests__/helpers';

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

type State = { balance: number };


describe('Aggregate#process', () => {
  const eventHandler: EventHandler<BalanceUpdatedEvent, State> = {
    type: EventType.BalanceUpdated,
    handle(_, event) {
      return {
        balance: event.body.balance,
      };
    },
  };

  const commandHandler: CommandHandler<UpdateBalanceCommand, BalanceUpdatedEvent, State> = {
    type: CommandType.UpdateBalance,
    handle(ctx, _, params) {
      const balance = ctx.state.balance + params.amount;

      if (balance < 0) {
        throw new Error('insufficient balance');
      }

      return {
        type: EventType.BalanceUpdated,
        body: { balance, amount: params.amount },
      };
    },
  };

  test.concurrent('process', async () => {
    const id = randomBytes(13);

    const EventStoreMock = {
      saveEvents: jest.fn().mockResolvedValue(undefined),
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator([])),
      getSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate<UpdateBalanceCommand, BalanceUpdatedEvent, State>(
      EventStoreMock as never as EventStore,
      [commandHandler],
      [eventHandler],
      id,
      0,
      { balance: 0 }
    );

    await aggregate.process({
      type: CommandType.UpdateBalance,
      args: [{ amount: 10 }],
    });

    expect(EventStoreMock.saveEvents).toBeCalledWith({
      aggregate: {
        id,
        version: 1,
      },
      timestamp: expect.any(Date),
      events: [
        {
          id: expect.any(EventId),
          type: EventType.BalanceUpdated,
          body: { balance: 10, amount: 10 },
          meta: {},
        },
      ],
    });
    expect(EventStoreMock.listEvents).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(EventStoreMock.getSnapshot).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(aggregate.state).toEqual({ balance: 10 });
    expect(aggregate.version).toEqual(1);
  });

  test.concurrent('invalid command', async () => {
    const id = randomBytes(13);

    const EventStoreMock = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator([])),
      getSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate<UpdateBalanceCommand, BalanceUpdatedEvent, State>(
      EventStoreMock as never as EventStore,
      [commandHandler],
      [eventHandler],
      id,
      0,
      { balance: 0 }
    );

    await expect(aggregate.process({
      type: CommandType.UpdateBalance,
      args: [{ amount: -10 }],
    })).rejects.toThrowError('insufficient balance');

    expect(EventStoreMock.listEvents).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(EventStoreMock.getSnapshot).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(aggregate.state).toEqual({ balance: 0 });
    expect(aggregate.version).toEqual(0);
  });

  test.concurrent('process multiple commands in succession', async () => {
    const id = randomBytes(13);
    const amounts = R.times(() => faker.datatype.number({ min: 10, max: 100, precision: 2 }), 10);

    const EventStoreMock = {
      saveEvents: jest.fn().mockResolvedValue(undefined),
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator([])),
      getSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate<UpdateBalanceCommand, BalanceUpdatedEvent, State>(
      EventStoreMock as never as EventStore,
      [commandHandler],
      [eventHandler],
      id,
      0,
      { balance: 0 }
    );

    for (const amount of amounts) {
      await aggregate.process({
        type: CommandType.UpdateBalance,
        args: [{ amount }],
      });
    }

    expect(EventStoreMock.saveEvents).toBeCalledTimes(amounts.length);
    expect(EventStoreMock.listEvents).toBeCalledTimes(amounts.length);
    expect(EventStoreMock.getSnapshot).toBeCalledTimes(amounts.length);
    expect(aggregate.state).toEqual({ balance: R.sum(amounts) });
    expect(aggregate.version).toEqual(amounts.length);
  });

  test.concurrent('aggregate version conflict', async () => {
    const id = randomBytes(13);

    const EventStoreMock = {
      saveEvents: jest.fn()
        .mockImplementationOnce(() =>
          Promise.reject(new AggregateVersionConflictError(id, 5))
        )
        .mockResolvedValueOnce(undefined),
      listEvents: jest.fn()
        .mockResolvedValueOnce(arrayToAsyncIterableIterator([]))
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
      getSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate<UpdateBalanceCommand, BalanceUpdatedEvent, State>(
      EventStoreMock as never as EventStore,
      [commandHandler],
      [eventHandler],
      id,
      4,
      { balance: 100 },
    );

    await aggregate.process({
      type: CommandType.UpdateBalance,
      args: [{ amount: 10 }],
    });

    expect(EventStoreMock.saveEvents).toBeCalledTimes(2);
    expect(EventStoreMock.listEvents).toBeCalledTimes(2);
    expect(aggregate.state).toEqual({ balance: 115 });
    expect(aggregate.version).toEqual(6);
  });

  test.concurrent('process multiple commands concurrently over multiple instances', async () => {
    const id = randomBytes(13);
    const events: Event[] = [
      {
        id: new EventId(),
        type: EventType.BalanceUpdated,
        aggregate: {
          id,
          version: 1,
        },
        body: { amount: 100 },
        meta: {},
        timestamp: new Date(),
      },
    ];

    const EventStoreMock = {
      saveEvents: jest.fn().mockImplementation(async (params) => {
        await setTimeout(50 + faker.datatype.number(100));

        const lastEvent = R.last(events);

        if (lastEvent && lastEvent.aggregate.version != params.aggregate.version - 1) {
          throw new AggregateVersionConflictError(
            id,
            lastEvent.aggregate.version
          );
        }

        for (const [index, event] of R.zip<number, Pick<Event, 'id' | 'type' | 'body'>>(
          R.range(0, params.events.length),
          params.events
        )) {
          events.push({
            ...event,
            timestamp: params.timestamp,
            aggregate: {
              id: params.aggregate.id,
              version: params.aggregate.version + index,
            },
            meta: {},
          });
        }
      }),
      listEvents: jest.fn().mockImplementation(async (params) => {
        await setTimeout(50 + faker.datatype.number(100));

        return arrayToAsyncIterableIterator(events.slice(params.aggregate.version));
      }),
      getSnapshot: jest.fn().mockResolvedValue(null),
    };

    await Promise.all(R.times(async () => {
      await setTimeout(10 + faker.datatype.number(50));

      const aggregate = new Aggregate<UpdateBalanceCommand, BalanceUpdatedEvent, State>(
        EventStoreMock as never as EventStore,
        [commandHandler],
        [eventHandler],
        id,
        0,
        { balance: 0 }
      );

      await aggregate.process({
        type: CommandType.UpdateBalance,
        args: [{ amount: 10 }],
      });

      return aggregate;
    }, 20));

    expect(events.length).toEqual(21);
  });
});