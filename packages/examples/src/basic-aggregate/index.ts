import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { getRandomPort } from 'get-port-please';
import { MongoStoreAdapter } from '@arque/mongo-store-adapter';
import { FakeStreamAdapter } from '../libs/fake-stream-adapter';
import { AggregateFactory } from '@arque/core';
import {
  WalletAggregate,
  WalletAggregateCommandHandlers,
  WalletAggregateCommandType,
  WalletAggregateEventHandlers
} from '../libs/wallet-aggregate';
import Decimal from 'decimal.js';
import { randomBytes } from 'crypto';
import ora, { Ora } from 'ora';
import assert from 'assert';
import R from 'ramda';

export async function execute() {
  let spinner: Ora;

  spinner = ora('starting MongoDB server').start();

  const mongo = await MongoMemoryReplSet.create({
    replSet: {
      storageEngine: 'wiredTiger',
      count: 3,
    },
    instanceOpts: [
      {
        launchTimeout: 30000,
        port: await getRandomPort(),
      },
    ],
  });

  spinner.succeed('MongoDB server started');

  const store = new MongoStoreAdapter({
    uri: await mongo.getUri(),
    serializers: [
      {
        serialize: (value: Decimal) => value.toString(),
        deserialize: (value: string) => new Decimal(value),
        type: Decimal,
      }
    ],
  });

  const WalletAggregateFactory = new AggregateFactory<WalletAggregate>(
    store,
    new FakeStreamAdapter(),
    WalletAggregateCommandHandlers,
    WalletAggregateEventHandlers,
    {
      defaultState: {
        balance: new Decimal(10),
      },
    }
  );

  const aggregate = await WalletAggregateFactory.load(randomBytes(13));

  await aggregate.process({
    type: WalletAggregateCommandType.AddTransaction,
    args: [
      [
        {
          type: 'debit',
          params: {
            amount: new Decimal(10),
          },
        },
        {
          type: 'credit',
          params: {
            amount: new Decimal(15),
          },
        },
        {
          type: 'credit',
          params: {
            amount: new Decimal(5),
          },
        },
        {
          type: 'debit',
          params: {
            amount: new Decimal(15),
          },
        },
      ],
    ],
  });

  for (const _ of R.range(0, 100)) {
    console.time('process');
    await aggregate.process({
      type: WalletAggregateCommandType.Credit,
      args: [
        {
          amount: new Decimal(10),
        },
      ],
    });

    await aggregate.process({
      type: WalletAggregateCommandType.Debit,
      args: [
        {
          amount: new Decimal(5),
        },
      ],
    });
    console.timeEnd('process');
  }

  assert(aggregate.state.balance.equals(new Decimal(505)));
  assert(aggregate.version === 204);

  await store.close();

  await mongo.stop();
}