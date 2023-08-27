
import { Schema } from 'mongoose';

const Stream = new Schema({
  name: String,
  events: [Number],
}, {
  autoIndex: true,
});
Stream.index({ 'name': 1 }, { unique: true });
Stream.index({ 'events': 1 });


export { Stream };