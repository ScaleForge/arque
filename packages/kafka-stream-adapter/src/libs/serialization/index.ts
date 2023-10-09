import { EventId, Event } from '@arque/core';
import { Builder, ByteBuffer } from 'flatbuffers';
import { Event as FlatbuffersEvent } from './flatbuffers/event_generated';
import { Joser } from '@scaleforge/joser';

export function serialize(event: Event, joser: Joser): Buffer {
  const builder = new Builder(1024);
  
  const id = FlatbuffersEvent.createIdVector(builder, event.id.buffer);
  const aggregate_id = FlatbuffersEvent.createAggregateIdVector(builder, event.aggregate.id);
  const body = FlatbuffersEvent.createBodyVector(builder, Buffer.from(JSON.stringify(joser.serialize(event.body)), 'utf8'));
  const meta = FlatbuffersEvent.createMetaVector(builder, Buffer.from(JSON.stringify(joser.serialize(event.meta)), 'utf8'));

  FlatbuffersEvent.startEvent(builder);

  FlatbuffersEvent.addId(builder, id);
  FlatbuffersEvent.addType(builder, event.type);
  FlatbuffersEvent.addAggregateId(builder, aggregate_id);
  FlatbuffersEvent.addAggregateVersion(builder, event.aggregate.version);
  FlatbuffersEvent.addBody(builder, body);
  FlatbuffersEvent.addMeta(builder, meta);
  FlatbuffersEvent.addTimestamp(builder, Math.floor(event.timestamp.getTime() / 1000));

  const offset = FlatbuffersEvent.endEvent(builder);

  builder.finish(offset);

  return Buffer.from(builder.asUint8Array());
}

export function deserialize(data: Buffer, joser: Joser):
  Pick<Event, 'id' | 'type' | 'aggregate' | 'meta' | 'timestamp'> & { body: Record<string, unknown> | null } {
  const buffer = new ByteBuffer(data);

  const event = FlatbuffersEvent.getRootAsEvent(buffer);

  const rawBody = event.bodyArray();

  const body = rawBody ? joser.deserialize(JSON.parse(Buffer.from(rawBody).toString('utf8'))) : null;

  return {
    id: EventId.from(Buffer.from(event.idArray())),
    type: event.type(),
    aggregate: {
      id: Buffer.from(event.aggregateIdArray()),
      version: event.aggregateVersion(),
    },
    body,
    meta: joser.deserialize(JSON.parse(Buffer.from(event.metaArray()).toString('utf8'))),
    timestamp: new Date(event.timestamp() * 1000),
  };
}