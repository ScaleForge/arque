import { EventId } from './event-id';

export type Meta<T extends Record<string, unknown> = Record<string, unknown>> = {
  __ctx?: Buffer;
} & T;

export type Event<
  TType extends number = number,
  TBody = Record<string, unknown>,
  TMeta extends Meta = Meta,
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

export type EventHandler<TEvent extends Event = Event, TState = unknown> = {
  type: TEvent['type'];
  handle(
    ctx: {
      readonly aggregate: {
        readonly id: Buffer;
        readonly version: number;
      },
      readonly state: TState;
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
> & Partial<Pick<TEvent, 'meta'>>;

export type CommandHandler<
  TCommand extends Command = Command,
  TEvent extends Event = Event,
  TState = unknown,
> = {
  type: TCommand['type'];
  handle(
    ctx: {
      readonly aggregate: {
        readonly id: Buffer;
        readonly version: number;
      },
      readonly state: TState;
    },
    command: TCommand,
    ...args: TCommand['args']
  ):
    | GeneratedEvent<TEvent>
    | GeneratedEvent<TEvent>[]
    | Promise<GeneratedEvent<TEvent>>
    | Promise<GeneratedEvent<TEvent>[]>;
};

export type ProjectionEventHandler<TEvent extends Event = Event, TState = unknown> = {
  type: TEvent['type'];
  handle(ctx: { readonly state: TState }, event: TEvent): void | Promise<void>;
};