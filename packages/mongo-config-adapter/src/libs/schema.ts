import { Schema } from 'mongoose';

const Stream = new Schema({
  _id: String,
  events: [Number],
  timestamp: Date,
}, {
  id: false,
  autoIndex: true,
});
Stream.index({ events: 1 });

export { Stream };