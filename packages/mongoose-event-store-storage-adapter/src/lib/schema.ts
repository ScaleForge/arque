import { EventId } from '@arque/core';
import { Schema } from 'mongoose';

const EventSchema = new Schema({
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
EventSchema.index({ 'aggregate.id': 1, 'aggregate.version': 1 }, { unique: true });

export { EventSchema };