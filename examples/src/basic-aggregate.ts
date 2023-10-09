import { Aggregate, AggregateFactory } from '@arque/core';
import { MongoStoreAdapter } from '@arque/mongo-store-adapter';
import { KafkaStreamAdapter } from '@arque/kafka-stream-adapter';
import { randomBytes } from 'crypto';
import {
  EventType,
  WalletAggregateCommandHandler,
  WalletAggregateCommandType,
  WalletAggregateEventHandler,
  WalletAggregateState,
} from './libs/common';
import debug from 'debug';
import assert from 'assert';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092,localhost:9093,localhost:9094').split(',');

const MONGODB_URI = process.env.MONGODB_URI ?? 'mongodb://mongo1:27021,mongo2:27022,mongo3:27023/?replicaSet=rs0';

type WalletAggregate = Aggregate<WalletAggregateState, WalletAggregateCommandHandler, WalletAggregateEventHandler>;

async function main() {
  const store = new MongoStoreAdapter({
    uri: MONGODB_URI,
  });

  const stream = new KafkaStreamAdapter({
    brokers: KAFKA_BROKERS,
  });

  const WalletAggregateFactory = new AggregateFactory<WalletAggregate>(
    store,
    stream,
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
    store.close(),
    stream.close(),
  ]);
}

main().then(() => {
  debug('done');
}).catch(err => {
  debug(err);
});
