import { AggregateFactory } from '@arque/core';
import { MongoStoreAdapter } from '@arque/mongo-store-adapter';
import { KafkaStreamAdapter } from '@arque/kafka-stream-adapter';
import { randomBytes } from 'crypto';
import {
  WalletAggregate,
  WalletAggregateCommandHandlers,
  WalletAggregateCommandType,
  WalletAggregateEventHandlers,
} from './libs/common';
import debug from 'debug';
import assert from 'assert';
import { MONGODB_URI, KAFKA_BROKERS } from './libs/config';

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
    WalletAggregateCommandHandlers,
    WalletAggregateEventHandlers,
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
