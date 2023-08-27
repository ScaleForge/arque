import { EventId } from '@arque/core';
import { Schema } from 'mongoose';

const Event = new Schema({
  _id: Buffer,
  type: Number,
  aggregate: {
    id: Buffer,
    version: Number,
  },
  body: Schema.Types.Mixed,
  timestamp: Date,
}, {
  id: false,
  autoIndex: true,
  virtuals: {
    id: {
      get() {
        return EventId.from(this._id);
      },
      set(value: EventId) {
        this._id = value.buffer;
      },
    },
  },
});
Event.index({ 'aggregate.id': 1, 'aggregate.version': 1 }, { unique: true });

const Aggregate = new Schema({
  _id: Buffer,
  version: Number,
  timestamp: Date,
}, {
  autoIndex: true,
});
Aggregate.index({ '_id': 1, 'version': 1 }, { unique: true });

const Snapshot = new Schema({
  aggregate: {
    id: Buffer,
    version: Number,
  },
  state: Schema.Types.Mixed,
  timestamp: Date,
}, {
  autoIndex: true,
});
Snapshot.index({ 'aggregate.id': 1, 'aggregate.version': 1 }, { unique: true });


export { Event, Aggregate, Snapshot };