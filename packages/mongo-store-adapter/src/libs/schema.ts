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
}, {
  id: false,
  autoIndex: true,
});
Event.index({ 'aggregate.id': 1, 'aggregate.version': 1 }, { unique: true, background: false });
Event.index({ 'type': 1, 'timestamp': 1 });

const Aggregate = new Schema({
  _id: Buffer,
  version: Number,
  timestamp: Date,
}, {
  id: false,
  autoIndex: true,
});
Aggregate.index({ '_id': 1, 'version': 1 }, { unique: true, background: false });

const Snapshot = new Schema({
  aggregate: {
    id: Buffer,
    version: Number,
  },
  state: Schema.Types.Mixed,
  timestamp: Date,
}, {
  id: false,
  autoIndex: true,
});
Snapshot.index({ 'aggregate.id': 1, 'aggregate.version': 1 }, { unique: true, background: false });

const ProjectionCheckpoint = new Schema({
  projection: String,
  aggregate: {
    id: Buffer,
    version: Number,
  },
  timestamp: Date,
}, {
  id: false,
  autoIndex: true,
});
ProjectionCheckpoint.index({ 'projection': 1, 'aggregate.id': 1 }, { unique: true, background: false });
ProjectionCheckpoint.index({ 'projection': 1, 'aggregate.id': 1, 'aggregate.version': 1 });

export { Event, Aggregate, Snapshot, ProjectionCheckpoint };
