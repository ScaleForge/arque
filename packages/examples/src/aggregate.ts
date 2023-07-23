import { AggregateFactory, EventStore, StorageAdapter, StreamAdapter, Event, Command, CommandHandler, EventHandler } from '@arque/core';
import { MongoStorageAdapter } from '@arque/mongo-storage-adapter';
import { KafkaStreamAdapter } from '@arque/kafka-stream-adapter';
import { randomBytes } from 'crypto';
import assert from 'assert';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092,localhost:9093,localhost:9094').split(',');

const MONGODB_URI = process.env.MONGODB_URI ?? 'mongodb://mongo1:27021,mongo2:27022,mongo3:27023/?replicaSet=rs0';

enum EventType {
  WalletDebited = 0,
  WalletCredited = 1,
}

type WalletDebitedEvent = Event<EventType.WalletDebited, {
  amount: number;
  balance: number;
}>;

type WalletCreditedEvent = Event<EventType.WalletCredited, {
  amount: number;
  balance: number;
}>;

enum WalletAggregateCommandType {
  Debit = 0,
  Credit = 1,
  AddTransaction = 2,
}

type WalletAggregateState = {
  balance: number;
};

type WalletDebitCommandHandler = CommandHandler<
  Command<WalletAggregateCommandType.Debit, [{ amount: number }]>,
  WalletDebitedEvent,
  WalletAggregateState
>;

type WalletCreditCommandHandler = CommandHandler<
  Command<WalletAggregateCommandType.Credit, [{ amount: number }]>,
  WalletCreditedEvent,
  WalletAggregateState
>;

type WalletAddTransactionCommandHandler = CommandHandler<
  Command<WalletAggregateCommandType.AddTransaction, [{ type: 'debit' | 'credit', amount: number }[]]>,
  WalletDebitedEvent | WalletCreditedEvent,
  WalletAggregateState
>;

type WalletAggregateCommandHandler = WalletDebitCommandHandler | WalletCreditCommandHandler | WalletAddTransactionCommandHandler;

type WalletDebitedEventHandler = EventHandler<WalletDebitedEvent, WalletAggregateState>;

type WalletCreditedEventHandler = EventHandler<WalletCreditedEvent, WalletAggregateState>;

type WalletAggregateEventHandler = WalletDebitedEventHandler | WalletCreditedEventHandler;

async function main() {
  const storage: StorageAdapter = new MongoStorageAdapter({
    uri: MONGODB_URI,
  });

  const stream: StreamAdapter = new KafkaStreamAdapter({
    brokers: KAFKA_BROKERS,
  });

  const store = new EventStore(storage, stream);

  const WalletAggregateFactory = new AggregateFactory<WalletAggregateCommandHandler, WalletAggregateEventHandler, WalletAggregateState>(
    store,
    [
      {
        type: WalletAggregateCommandType.Debit,
        handle(ctx, _command, { amount }) {
          const balance = ctx.state.balance - amount;

          if (balance < 0) {
            throw new Error('not enough balance');
          }

          return {
            type: EventType.WalletDebited,
            body: {
              amount,
              balance,
            },
          };
        },
      },
      {
        type: WalletAggregateCommandType.Credit,
        handle(ctx, _command, { amount }) {
          return {
            type: EventType.WalletCredited,
            body: {
              amount,
              balance: ctx.state.balance + amount,
            },
          };
        },
      },
      {
        type: WalletAggregateCommandType.AddTransaction,
        handle(ctx, _command, operations) {
          const events = [];

          let balance = ctx.state.balance;

          for (const operation of operations) {
            if (operation.type === 'debit') {
              balance -= operation.amount;

              if (balance < 0) {
                throw new Error('not enough balance');
              }
    
              events.push({
                type: EventType.WalletDebited,
                body: {
                  amount: operation.amount,
                  balance,
                },
              });

              continue;
            }

            balance += operation.amount;

            events.push({
              type: EventType.WalletCredited,
              body: {
                amount: operation.amount,
                balance,
              },
            });
          }

          return events;
        },
      },
    ],
    [
      {
        type: EventType.WalletCredited,
        handle(_ctx, event) {
          return {
            balance: event.body.balance,
          };
        },
      },
      {
        type: EventType.WalletDebited,
        handle(_ctx, event) {
          return {
            balance: event.body.balance,
          };
        },
      },
    ],
    {
      defaultState: {
        balance: 0,
      },
    }
  );

  const aggregate = await WalletAggregateFactory.load(randomBytes(13));

  await aggregate.process({
    type: WalletAggregateCommandType.Credit,
    args: [
      {
        amount: 10,
      },
    ],
  });

  await aggregate.process({
    type: WalletAggregateCommandType.Credit,
    args: [
      {
        amount: 100,
      },
    ],
  });

  await aggregate.process({
    type: WalletAggregateCommandType.Debit,
    args: [
      {
        amount: 60,
      },
    ],
  });

  await aggregate.process({
    type: WalletAggregateCommandType.Credit,
    args: [
      {
        amount: 10,
      },
    ],
  });

  await aggregate.process({
    type: WalletAggregateCommandType.AddTransaction,
    args: [
      [
        {
          type: 'debit',
          amount: 10,
        },
        {
          type: 'credit',
          amount: 5,
        },
        {
          type: 'credit',
          amount: 5,
        },
        {
          type: 'debit',
          amount: 15,
        },
      ],
    ],
  });

  assert(aggregate.state.balance === 45);
  assert(aggregate.version === 8);

  await Promise.all([
    storage.close(),
    stream.close(),
  ]);
}

main().then(() => {
  console.log('done');
}).catch(err => {
  console.error(err);
});
