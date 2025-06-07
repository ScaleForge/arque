import bs58 from 'bs58';
import { randomBytes } from 'crypto';

export class EventId {
  private _buffer: Buffer;

  private static random = randomBytes(5);

  private static counter = randomBytes(4).readUInt32BE() & 0x00ffffff;

  constructor(value?: Buffer) {
    if (value) {
      this._buffer = value;
      return;
    }

    this._buffer = Buffer.alloc(12, 0);

    const timestamp = Math.floor(Date.now() / 1000) & 0xffffffff;

    EventId.counter = (EventId.counter + 1) & 0x00ffffff;

    this._buffer[0] = (timestamp >> 24) & 0xff;
    this._buffer[1] = (timestamp >> 16) & 0xff;
    this._buffer[2] = (timestamp >> 8) & 0xff;
    this._buffer[3] = timestamp & 0xff;
    this._buffer[4] = EventId.random[0];
    this._buffer[5] = EventId.random[1];
    this._buffer[6] = EventId.random[2];
    this._buffer[7] = EventId.random[3];
    this._buffer[8] = EventId.random[4];
    this._buffer[9] = (EventId.counter >> 16) & 0xff;
    this._buffer[10] = (EventId.counter >> 8) & 0xff;
    this._buffer[11] = EventId.counter & 0xff;
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

  public static from(value: string | Buffer) {
    if (value instanceof Buffer) {
      return new EventId(value);
    }

    return new EventId(Buffer.from(bs58.decode(<string>value)));
  }

  public static generate() {
    return new EventId();
  }

  public toString(encoding: 'bs58' | 'hex' | 'base64' = 'bs58') {
    if (encoding === 'bs58') {
      return bs58.encode(this._buffer);
    }

    return this._buffer.toString(encoding);
  }
}
