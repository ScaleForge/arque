import { Event } from '../types';

export interface Subscriber {
  stop(): Promise<void>;
}

export interface StreamAdapter {
  sendEvents(events: Event[], ctx?: Buffer): Promise<void>;

  subscribe(params: {
    stream: string,
    handle: (event: Event) => Promise<void>
  }): Promise<Subscriber>;
  
  close(): Promise<void>;
}
