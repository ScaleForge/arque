import { AggregateFactory, Broker, Projection, ProjectionEventHandler } from '@arque/core';
import { MongoStoreAdapter } from '@arque/mongo-store-adapter';
import { KafkaStreamAdapter } from '@arque/kafka-stream-adapter';
import { MongoConfigAdapter } from '@arque/mongo-config-adapter';
import { randomBytes } from 'crypto';
import {
  EventType,
  WalletAggregate,
  WalletAggregateCommandHandlers,
  WalletAggregateCommandType,
  WalletAggregateEventHandlers,
  WalletCreditedEvent,
  WalletDebitedEvent,
} from './libs/common';
import debug from 'debug';
import { MONGODB_URI, KAFKA_BROKERS } from './libs/config';

type WalletProjectionState = { balance: number };

type WalletCreditedProjectionEventHandler = ProjectionEventHandler<WalletCreditedEvent, WalletProjectionState>;
type WalletDebitedProjectionEventHandler = ProjectionEventHandler<WalletDebitedEvent, WalletProjectionState>;

async function main() {
  const store = new MongoStoreAdapter({
    uri: MONGODB_URI,
  });

  const stream = new KafkaStreamAdapter({
    brokers: KAFKA_BROKERS,
  });

  const config = new MongoConfigAdapter({
    uri: MONGODB_URI,
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

  const projection = new Projection<WalletProjectionState, WalletCreditedProjectionEventHandler | WalletDebitedProjectionEventHandler>(
    store,
    stream,
    config,
    [
      {
        type: EventType.WalletCredited,
        handle: (ctx, event) => {
          console.log('WalletCredited');
          ctx.state.balance = event.body.balance;
        },
      },
      {
        type: EventType.WalletDebited,
        handle: (ctx, event) => {
          console.log('WalletDebited');
          ctx.state.balance = event.body.balance;
        },
      },
    ],
    'wallet',
    {
      balance: 0,
    },
  );

  await projection.start();

  const broker = new Broker(config, stream);

  await broker.start();

  const id = randomBytes(13);

  const aggregate = await WalletAggregateFactory.load(id);

  await aggregate.process({
    type: WalletAggregateCommandType.Credit,
    args: [
      {
        amount: 10,
      },
    ],
  });

  await aggregate.process({
    type: WalletAggregateCommandType.Debit,
    args: [
      {
        amount: 5,
      },
    ],
  });

  await projection.waitUntilSettled(10000);

  await projection.stop();
  await broker.stop();
  await config.close();
  await store.close();
  await stream.close();
}

main().then(() => {
  debug('done');
}).catch(err => {
  debug(err);
});
