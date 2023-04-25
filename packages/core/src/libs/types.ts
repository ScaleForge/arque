import { EventId } from './event-id';

export type Event<
  TType extends number = number,
  TBody extends Record<string, unknown> = Record<string, unknown>,
  TMeta extends Record<string, unknown> = Record<string, unknown>,
> = {
  id: EventId;
  type: TType;
  aggregate: {
    id: Buffer;
    version: number;
  };
  body: TBody;
  meta: TMeta;
  timestamp: Date;
};

export type EventHandler<TEvent extends Event, TState> = {
  type: TEvent['type'];
  handle(
    ctx: {
      aggregate: {
        id: Buffer;
        version: number;
      },
      state: TState;
    },
    event: TEvent
  ): TState | Promise<TState>;
};

export type Command<
  TType extends number = number,
  TArgs extends Array<unknown> = Array<unknown>,
> = {
  type: TType;
  args: TArgs;
};

export type GeneratedEvent<TEvent extends Event> = Pick<
  TEvent,
  'type' | 'body'
> &
  Partial<Pick<TEvent, 'meta'>>;

export type CommandHandler<
  TCommand extends Command,
  TEvent extends Event,
  TState,
> = {
  type: TCommand['type'];
  handle(
    ctx: {
      aggregate: {
        id: Buffer;
        version: number;
      },
      state: TState;
    },
    command: TCommand,
    ...args: TCommand['args']
  ):
    | GeneratedEvent<TEvent>
    | GeneratedEvent<TEvent>[]
    | Promise<GeneratedEvent<TEvent>>
    | Promise<GeneratedEvent<TEvent>[]>;
};

export type Snapshot<TState = unknown> = {
  aggregate: {
    id: Buffer;
    version: number;
  };
  state: TState;
  timestamp: Date;
};