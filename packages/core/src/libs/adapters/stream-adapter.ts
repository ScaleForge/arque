import { Event } from '../types';

export interface Subscriber {
  stop(): Promise<void>;
}

export type StreamEvent = Pick<Event, 'id' | 'type' | 'aggregate' | 'timestamp'> & { meta: {
  __ctx?: Buffer;
} };

export interface StreamAdapter {
  sendEvents(params: { events: StreamEvent[] }): Promise<void>;

  subscribe(params: {
    stream: string,
    handle: (event: Event) => Promise<void>
  }): Promise<Subscriber>;
  
  close(): Promise<void>;
}
