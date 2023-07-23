import { EventId } from '@arque/core';
import { randomBytes } from 'crypto';
import { serialize, deserialize } from '.';

describe('serialization', () => {
  const cases = [
    {
      id: new EventId(),
      type: randomBytes(2).readUint16BE(),
      aggregate: {
        id: randomBytes(13),
        version: randomBytes(4).readUint32BE(),
      },
      meta: {
        __ctx: randomBytes(13),
      },
      timestamp: new Date(Math.floor(Date.now() / 1000) * 1000),
    },
    {
      id: new EventId(),
      type: randomBytes(2).readUint16BE(),
      aggregate: {
        id: randomBytes(13),
        version: randomBytes(4).readUint32BE(),
      },
      meta: {},
      timestamp: new Date(Math.floor(Date.now() / 1000) * 1000),
    },
  ];

  test.each(cases)('serialize and deserialize', (input) => {
    const result = deserialize(serialize(input));

    expect(result).toMatchObject(input);
  });
});