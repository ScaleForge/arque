import { AggregateFactory, EventStore } from '@arque/core';
import { MongoStorageAdapter } from '@arque/mongo-storage-adapter';
import { KafkaStreamAdapter } from '@arque/kafka-stream-adapter';
import { MongoConfigAdapter } from '@arque/mongo-config-adapter';
import { randomBytes } from 'crypto';
import assert from 'assert';
import {
  EventType,
  WalletAggregateCommandHandler,
  WalletAggregateCommandType,
  WalletAggregateEventHandler,
  WalletAggregateState,
} from './libs/common';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092,localhost:9093,localhost:9094').split(',');

const MONGODB_URI = process.env.MONGODB_URI ?? 'mongodb://mongo1:27021,mongo2:27022,mongo3:27023/?replicaSet=rs0';

async function main() {
  const storage = new MongoStorageAdapter({
    uri: MONGODB_URI,
  });

  const stream = new KafkaStreamAdapter({
    brokers: KAFKA_BROKERS,
  });

  const config = new MongoConfigAdapter({
    uri: MONGODB_URI,
  });

  const store = new EventStore(storage, stream, config);

  const WalletAggregateFactory = new AggregateFactory<WalletAggregateState, WalletAggregateCommandHandler, WalletAggregateEventHandler>(
    store,
    [
      {
        type: WalletAggregateCommandType.Debit,
        handle(ctx, _, { amount }) {
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
        handle(ctx, _, { amount }) {
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
        type: WalletAggregateCommandType.CreateTransaction,
        handle(ctx, _, operations) {
          const events = [];

          let balance = ctx.state.balance;

          for (const operation of operations) {
            if (operation.type === 'debit') {
              balance -= operation.params.amount;

              if (balance < 0) {
                throw new Error('not enough balance');
              }
    
              events.push({
                type: EventType.WalletDebited,
                body: {
                  amount: operation.params.amount,
                  balance,
                },
              });

              continue;
            }

            balance += operation.params.amount;

            events.push({
              type: EventType.WalletCredited,
              body: {
                amount: operation.params.amount,
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
    type: WalletAggregateCommandType.CreateTransaction,
    args: [
      [
        {
          type: 'debit',
          params: {
            amount: 10,
          },
        },
        {
          type: 'credit',
          params: {
            amount: 5,
          },
        },
        {
          type: 'credit',
          params: {
            amount: 5,
          },
        },
        {
          type: 'debit',
          params: {
            amount: 15,
          },
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
