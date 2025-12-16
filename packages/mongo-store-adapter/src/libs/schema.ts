import { Schema } from 'mongoose';

const Event = new Schema({
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
Event.index({ 'aggregate.id': 'hashed' });
Event.index({ 'aggregate.id': 1, 'aggregate.version': 1 });
Event.index({ 'type': 1, 'timestamp': 1 });

const Aggregate = new Schema({
  _id: Buffer,
  version: Number,
  timestamp: Date,
  final: Boolean,
}, {
  id: false,
});
Aggregate.index({ '_id': 'hashed' });

const Snapshot = new Schema({
  aggregate: {
    id: Buffer,
    version: Number,
  },
  state: Schema.Types.Mixed,
  timestamp: Date,
}, {
  id: false,
});
Snapshot.index({ 'aggregate.id': 'hashed' });
Snapshot.index({ 'aggregate.id': 1, 'aggregate.version': -1 });

const ProjectionCheckpoint = new Schema({
  projection: String,
  aggregate: {
    id: Buffer,
    version: Number,
  },
  timestamp: Date,
}, {
  id: false,
});
ProjectionCheckpoint.index({ 'projection': 1, 'aggregate.id': 'hashed' });
ProjectionCheckpoint.index({ 'timestamp': 1 }, { expireAfterSeconds: 60 * 60 * 24 * 7 });

export { Event, Aggregate, Snapshot, ProjectionCheckpoint };
