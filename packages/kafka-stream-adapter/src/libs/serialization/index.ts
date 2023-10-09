import { EventId, StreamEvent } from '@arque/core';
import { Builder, ByteBuffer } from 'flatbuffers';
import { Event as FlatbuffersEvent } from './flatbuffers/event_generated';

export function serialize(event: StreamEvent): Buffer {
  const builder = new Builder(1024);

  const id = FlatbuffersEvent.createIdVector(builder, event.id.buffer);
  const aggregate_id = FlatbuffersEvent.createAggregateIdVector(builder, event.aggregate.id);
  const meta___ctx = event.meta.__ctx ? FlatbuffersEvent.createMeta_CtxVector(builder, event.meta.__ctx) : null;

  FlatbuffersEvent.startEvent(builder);

  FlatbuffersEvent.addId(builder, id);
  FlatbuffersEvent.addType(builder, event.type);
  FlatbuffersEvent.addAggregateId(builder, aggregate_id);
  FlatbuffersEvent.addAggregateVersion(builder, event.aggregate.version);
  if (meta___ctx !== null) {
    FlatbuffersEvent.addMeta_Ctx(builder, meta___ctx);
  }
  FlatbuffersEvent.addTimestamp(builder, Math.floor(event.timestamp.getTime() / 1000));

  const offset = FlatbuffersEvent.endEvent(builder);

  builder.finish(offset);

  return Buffer.from(builder.asUint8Array());
}

export function deserialize(data: Buffer): StreamEvent {
  const buffer = new ByteBuffer(data);

  const event = FlatbuffersEvent.getRootAsEvent(buffer);

  const meta___ctx = event.meta_CtxArray();

  const meta: { __ctx?: Buffer } = {};

  if (meta___ctx) {
    meta.__ctx = Buffer.from(meta___ctx);
  }

  return {
    id: EventId.from(Buffer.from(event.idArray())),
    type: event.type(),
    aggregate: {
      id: Buffer.from(event.aggregateIdArray()),
      version: event.aggregateVersion(),
    },
    meta,
    timestamp: new Date(event.timestamp() * 1000),
  };
}