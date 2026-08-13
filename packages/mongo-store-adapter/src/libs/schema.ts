import { Schema } from 'mongoose';

export type EventDocument = {
  _id: Buffer;
  type: number;
  aggregate: {
    id: Buffer;
    version: number;
  };
  body: Record<string, unknown>;
  meta: Record<string, unknown>;
  timestamp: Date;
  final: boolean;
};

const EventSchema = new Schema<EventDocument>({
  _id: Buffer,
  type: Number,
  aggregate: {
    id: Buffer,
    version: Number,
  },
  body: Schema.Types.Mixed,
  meta: Schema.Types.Mixed,
  timestamp: Date,
  final: Boolean,
}, {
  id: false,
});
EventSchema.index({ 'aggregate.id': 'hashed' });
EventSchema.index({ 'aggregate.id': 1, 'aggregate.version': 1 });
EventSchema.index({ 'type': 1, 'timestamp': 1 });

export type AggregateDocument = {
  _id: Buffer;
  version: number;
  timestamp: Date;
  final?: boolean;
};

const AggregateSchema = new Schema<AggregateDocument>({
  _id: Buffer,
  version: Number,
  timestamp: Date,
  final: Boolean,
}, {
  id: false,
});
AggregateSchema.index({ '_id': 'hashed' });

export type SnapshotDocument = {
  aggregate: {
    id: Buffer;
    version: number;
  };
  state: Record<string, unknown>;
  timestamp: Date;
};

const SnapshotSchema = new Schema<SnapshotDocument>({
  aggregate: {
    id: Buffer,
    version: Number,
  },
  state: Schema.Types.Mixed,
  timestamp: Date,
}, {
  id: false,
});
SnapshotSchema.index({ 'aggregate.id': 'hashed' });
SnapshotSchema.index({ 'aggregate.id': 1, 'aggregate.version': 1 });

export type ProjectionCheckpointDocument = {
  projection: string;
  aggregate: {
    id: Buffer;
    version: number;
  };
  timestamp: Date;
};

const ProjectionCheckpointSchema = new Schema<ProjectionCheckpointDocument>({
  projection: String,
  aggregate: {
    id: Buffer,
    version: Number,
  },
  timestamp: Date,
}, {
  id: false,
});
ProjectionCheckpointSchema.index({ 'projection': 1, 'aggregate.id': 'hashed' });
ProjectionCheckpointSchema.index({ 'timestamp': 1 }, { expireAfterSeconds: 60 * 60 * 24 * 7 });

export type LockDocument = {
  _id: Buffer;
  owner: Buffer;
  timestamp: Date;
};

const LockSchema = new Schema<LockDocument>({
  _id: Buffer,
  owner: Buffer,
  timestamp: Date,
}, {
  id: false,
  collection: 'locks',
});
LockSchema.index({ 'timestamp': 1 }, { expireAfterSeconds: 30 });

export { EventSchema, AggregateSchema, SnapshotSchema, ProjectionCheckpointSchema, LockSchema };
