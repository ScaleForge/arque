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
import { MONGODB_URI, KAFKA_BROKERS } from './libs/config';
import R from 'ramda';
import assert from 'assert';

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
      snapshotInterval: 10,
    }
  );

  const id = randomBytes(13);

  const aggregate = await WalletAggregateFactory.load(id);

  const count = 35;

  for (const index of R.range(0, count)) {
    if (index % 2 === 0) {
      await aggregate.process({
        type: WalletAggregateCommandType.Credit,
        args: [
          {
            amount: 10,
          },
        ],
      });

      continue;
    }

    await aggregate.process({
      type: WalletAggregateCommandType.Debit,
      args: [
        {
          amount: 5,
        },
      ],
    });
  }

  WalletAggregateFactory.clear();

  await WalletAggregateFactory.load(id);

  assert(aggregate.state.balance === 10 * Math.ceil(count / 2) - Math.floor(count / 2) * 5);
  assert(aggregate.version === count);

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
