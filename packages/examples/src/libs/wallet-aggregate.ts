import { Event, Command, CommandHandler, EventHandler, Aggregate } from '@arque/core';
import { Decimal } from 'decimal.js';

enum AggregateType {
  Wallet = 1,
}

export enum EventType {
  WalletDebited = AggregateType.Wallet << 8 | 0,
  WalletCredited = AggregateType.Wallet << 8 | 1,
}

export type WalletDebitedEvent = Event<EventType.WalletDebited, {
  amount: Decimal;
  balance: Decimal;
}>;

export type WalletCreditedEvent = Event<EventType.WalletCredited, {
  amount: Decimal;
  balance: Decimal;
}>;

export enum WalletAggregateCommandType {
  Debit = 0,
  Credit = 1,
  AddTransaction = 2,
}

export type WalletAggregateState = {
  balance: Decimal;
};

type WalletOperation = {
  type: 'debit' | 'credit';
  params: { amount: Decimal }
};

type WalletDebitCommandHandler = CommandHandler<
  Command<WalletAggregateCommandType.Debit, [{ amount: Decimal }]>,
  WalletDebitedEvent,
  WalletAggregateState
>;

type WalletCreditCommandHandler = CommandHandler<
  Command<WalletAggregateCommandType.Credit, [{ amount: Decimal }]>,
  WalletCreditedEvent,
  WalletAggregateState
>;

type WalletAddTransactionCommandHandler = CommandHandler<
  Command<WalletAggregateCommandType.AddTransaction, [WalletOperation[]]>,
  WalletDebitedEvent | WalletCreditedEvent,
  WalletAggregateState
>;

type WalletDebitedEventHandler = EventHandler<WalletDebitedEvent, WalletAggregateState>;

type WalletCreditedEventHandler = EventHandler<WalletCreditedEvent, WalletAggregateState>;

export type WalletAggregateCommandHandler = WalletDebitCommandHandler | WalletCreditCommandHandler | WalletAddTransactionCommandHandler;

export type WalletAggregateEventHandler = WalletDebitedEventHandler | WalletCreditedEventHandler;

export type WalletAggregate = Aggregate<WalletAggregateState, WalletAggregateCommandHandler, WalletAggregateEventHandler>;

export const WalletAggregateCommandHandlers: WalletAggregateCommandHandler[] = [
  {
    type: WalletAggregateCommandType.Debit,
    handle(ctx, _, { amount }) {
      const balance = ctx.state.balance.minus(amount);

      if (balance.lt(0)) {
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
          balance: ctx.state.balance.plus(amount),
        },
      };
    },
  },
  {
    type: WalletAggregateCommandType.AddTransaction,
    handle(ctx, _, operations) {
      const events = [];

      let balance = ctx.state.balance;

      for (const operation of operations) {
        if (operation.type === 'debit') {
          balance = balance.minus(operation.params.amount);

          if (balance.lt(0)) {
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

        balance = balance.plus(operation.params.amount);

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
];

export const WalletAggregateEventHandlers: WalletAggregateEventHandler[] = [
  {
    type: EventType.WalletCredited,
    handle(_, event) {
      return {
        balance: event.body.balance,
      };
    },
  },
  {
    type: EventType.WalletDebited,
    handle(_, event) {
      return {
        balance: event.body.balance,
      };
    },
  },
];