import { Event as GlobalEvent } from '@arque/core';

export type Event = Pick<GlobalEvent, 'id' | 'type' | 'aggregate' | 'meta' | 'timestamp'> & { body: Buffer | Record<string, unknown> | null };