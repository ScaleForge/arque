import { Event as GlobalEvent } from '../types';

type Event = Pick<GlobalEvent, 'id' | 'type' | 'aggregate' | 'meta' | 'timestamp'> & { body: Buffer | Record<string, unknown> | null };

export interface Subscriber {
  stop(): Promise<void>;
}

export interface StreamAdapter {
  init(): Promise<void>;

  sendEvents(
    events: {
      stream: string;
      events: Event[]; 
    }[],
    opts?: { raw?: true },
  ): Promise<void>;

  subscribe(
    stream: string,
    handle: (event: Event) => Promise<void>,
    opts?: { raw?: true, retry?: {
      maxDelay?: number;
      numOfAttempts?: number;
      retry?: (err: Error) => Promise<boolean>;
    } }): Promise<Subscriber>;
  
  close(): Promise<void>;
}
