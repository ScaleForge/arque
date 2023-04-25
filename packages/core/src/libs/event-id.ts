import bs58 from 'bs58';
import { randomBytes } from 'crypto';

const MACHINE_ID = randomBytes(5);

export class EventId {
  private _buffer: Buffer;

  private static counter = randomBytes(4).readUInt32BE() & 0xffffff;

  private static timestamp = Date.now();

  constructor(value?: Buffer) {
    if (value) {
      this._buffer = value;
      return;
    }

    this._buffer = Buffer.alloc(12, 0);

    if (EventId.counter === 0xffffff) {
      while(EventId.timestamp === this.timestamp()) { /* empty */ }
    }

    EventId.timestamp = this.timestamp();
    EventId.counter = (EventId.counter + 1) & 0xffffff;

    this._buffer[0] = (EventId.timestamp >> 24) & 0xff;
    this._buffer[1] = (EventId.timestamp >> 16) & 0xff;
    this._buffer[2] = (EventId.timestamp >> 8) & 0xff;
    this._buffer[3] = EventId.timestamp & 0xff;
    this._buffer[4] = MACHINE_ID[0];
    this._buffer[5] = MACHINE_ID[1];
    this._buffer[6] = MACHINE_ID[2];
    this._buffer[7] = MACHINE_ID[3];
    this._buffer[8] = MACHINE_ID[4];
    this._buffer[9] = (EventId.counter >> 16) & 0xff;
    this._buffer[10] = (EventId.counter >> 8) & 0xff;
    this._buffer[11] = EventId.counter & 0xff;
  }

  private timestamp() {
    return Math.floor(Date.now() / 1000) & 0xffffffff;
  }

  public get buffer() {
    return this._buffer;
  }

  public compare(other: EventId) {
    return this._buffer.compare(other._buffer);
  }

  public equals(other: EventId) {
    return this._buffer.equals(other._buffer);
  }

  public static from(value: Buffer | string) {
    if (value instanceof Buffer) {
      return new EventId(value);
    }

    return new EventId(Buffer.from(bs58.decode(value)));
  }

  public toString(encoding?: 'bs58' | 'hex' | 'base64') {
    if (!encoding || encoding === 'bs58') {
      return bs58.encode(this._buffer);
    }

    return this._buffer.toString(encoding);
  }
}
