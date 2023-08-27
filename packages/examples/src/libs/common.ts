import { Event, Command, CommandHandler, EventHandler } from '@arque/core';

enum AggregateType {
  Wallet = 0,
}

export enum EventType {
  WalletDebited = AggregateType.Wallet << 16 | 0,
  WalletCredited = (AggregateType.Wallet << 16 | 0) | 1,
}

type WalletDebitedEvent = Event<EventType.WalletDebited, {
  amount: number;
  balance: number;
}>;

type WalletCreditedEvent = Event<EventType.WalletCredited, {
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
