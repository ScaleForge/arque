import { Event, Command, CommandHandler, EventHandler, Aggregate } from '@arque/core';

enum AggregateType {
  Wallet = 1,
}

export enum EventType {
  WalletDebited = AggregateType.Wallet << 8 | 0,
  WalletCredited = AggregateType.Wallet << 8 | 1,
}

export type WalletDebitedEvent = Event<EventType.WalletDebited, {
  amount: number;
  balance: number;
}>;

export type WalletCreditedEvent = Event<EventType.WalletCredited, {
  amount: number;
  balance: number;
}>;

export enum WalletAggregateCommandType {
  Debit = 0,
  Credit = 1,
  CreateTransaction = 2,
}

export type WalletAggregateState = {
  balance: number;
};

type WalletOperation = {
  type: 'debit' | 'credit';
  params: { amount: number }
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

type WalletCreateTransactionCommandHandler = CommandHandler<
  Command<WalletAggregateCommandType.CreateTransaction, [WalletOperation[]]>,
  WalletDebitedEvent | WalletCreditedEvent,
  WalletAggregateState
>;

type WalletDebitedEventHandler = EventHandler<WalletDebitedEvent, WalletAggregateState>;

type WalletCreditedEventHandler = EventHandler<WalletCreditedEvent, WalletAggregateState>;

export type WalletAggregateCommandHandler = WalletDebitCommandHandler | WalletCreditCommandHandler | WalletCreateTransactionCommandHandler;

export type WalletAggregateEventHandler = WalletDebitedEventHandler | WalletCreditedEventHandler;

export type WalletAggregate = Aggregate<WalletAggregateState, WalletAggregateCommandHandler, WalletAggregateEventHandler>;

export const WalletAggregateCommandHandlers: WalletAggregateCommandHandler[] = [
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