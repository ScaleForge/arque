import { Event as GlobalEvent } from '../types';

type Event = Pick<GlobalEvent, 'id' | 'type' | 'aggregate' | 'meta' | 'timestamp'> & { body: Buffer | Record<string, unknown> | null };

export interface Subscriber {
  stop(): Promise<void>;
}

export interface StreamAdapter {
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
    opts?: { raw?: true }): Promise<Subscriber>;
  
  close(): Promise<void>;
}
