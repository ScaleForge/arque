import { Event } from './types';

export interface StreamReceiver {
  stop(): Promise<void>;
}

export type StreamEvent = Pick<Event, 'id' | 'type' | 'aggregate' | 'timestamp'> & { meta: {
  __ctx?: Buffer;
} };

export interface StreamAdapter {
  sendEvents(params: { events: StreamEvent[] }): Promise<void>;
  receiveEvents(
    stream: string,
    handler: (event: Event) => Promise<void>
  ): Promise<StreamReceiver>;
  close(): Promise<void>;
}
