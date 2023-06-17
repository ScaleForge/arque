import { randomBytes } from 'crypto';
import R from 'ramda';
import { Event, EventHandler, Command } from './types';
import { EventId } from './event-id';
import { Aggregate } from './aggregate';
import { EventStore } from './event-store';
import { arrayToAsyncIterableIterator } from '../../__tests__/helpers';

enum EventType {
  BalanceUpdated = 0,
}

type BalanceUpdatedEvent = Event<
  EventType.BalanceUpdated,
  { balance: number; amount: number }
>;

type State = { balance: number };

describe('Aggregate#reload', () => {
  const handler: EventHandler<BalanceUpdatedEvent, State> = {
    type: EventType.BalanceUpdated,
    handle(_, event: BalanceUpdatedEvent) {
      return {
        balance: event.body.balance,
      };
    },
  };

  test.concurrent('reload', async () => {
    const id = randomBytes(13);
    const amount = 10;
    const timestamp = new Date();
    const events = R.times((index) => ({
      id: new EventId(),
      type: EventType.BalanceUpdated,
      aggregate: {
        id,
        version: index + 1,
      },
      body: { balance: (index + 1) * amount, amount },
      timestamp,
    }), 10);

    const EventStoreMock = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator(events)),
      getSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate<Command<number, number[]>, BalanceUpdatedEvent, State>(
      EventStoreMock as never as EventStore,
      [],
      [handler],
      id,
      0,
      { balance: 0 }
    );

    await aggregate.reload();

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
    expect(aggregate.state).toEqual({ balance: events.length * amount });
    expect(aggregate.version).toEqual(events.length);
  });

  test.concurrent('reload multiple times, concurrently', async () => {
    const id = randomBytes(13);
    const amount = 10;

    const generateEvents = (offset: number) => {
      const timestamp = new Date();

      return R.times(
        (index) => ({
          id: new EventId(),
          type: EventType.BalanceUpdated,
          aggregate: {
            id: id.buffer,
            version: offset + index + 1,
          },
          body: { balance: (offset + index + 1) * amount, amount },
          timestamp,
        }),
        10
      );
    };

    const EventStoreMock = {
      listEvents: jest
        .fn()
        .mockImplementationOnce(async () => generateEvents(0))
        .mockImplementationOnce(async () => generateEvents(10))
        .mockImplementationOnce(async () => generateEvents(20)),
      getSnapshot: jest.fn().mockResolvedValue(null),
    };

    const aggregate = new Aggregate<Command<number, number[]>, BalanceUpdatedEvent, { balance: number }>(
      EventStoreMock as never as EventStore,
      [],
      [handler],
      id,
      0,
      { balance: 0 }
    );

    await Promise.all(R.times(() => aggregate.reload(), 3));

    expect(EventStoreMock.listEvents).toBeCalledTimes(3);
    expect(EventStoreMock.getSnapshot).toBeCalledTimes(3);
    expect(aggregate.state).toEqual({ balance: 30 * amount });
    expect(aggregate.version).toEqual(30);
  });

  test.concurrent('reload with snapshot', async () => {
    const timestamp = new Date();
    const id = randomBytes(13);
    const amount = 10;
    const events = R.times((index) => ({
      id: new EventId(),
      type: EventType.BalanceUpdated,
      aggregate: {
        id,
        version: 10 + index + 1,
      },
      body: { balance: 1000 + (index + 1) * amount, amount },
      timestamp,
    }), 10);

    const EventStoreMock = {
      listEvents: jest.fn().mockResolvedValue(arrayToAsyncIterableIterator(events)),
      getSnapshot: jest.fn().mockResolvedValue({
        aggregate: {
          id,
          version: 10,
        },
        state: { balance: 1000 },
        timestamp,
      }),
    };

    const aggregate = new Aggregate<Command<number, number[]>, BalanceUpdatedEvent, { balance: number }>(
      EventStoreMock as never as EventStore,
      [],
      [handler],
      id,
      0,
      { balance: 0 }
    );

    await aggregate.reload();

    expect(EventStoreMock.listEvents).toBeCalledWith({
      aggregate: {
        id,
        version: 10,
      },
    });
    expect(EventStoreMock.getSnapshot).toBeCalledWith({
      aggregate: {
        id,
        version: 0,
      },
    });
    expect(aggregate.state).toEqual({ balance: events.length * amount + 1000 });
    expect(aggregate.version).toEqual(events.length + 10);
  });
});
