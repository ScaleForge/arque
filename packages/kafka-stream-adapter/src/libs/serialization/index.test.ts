import { EventId } from '@arque/core';
import { randomBytes } from 'crypto';
import { serialize, deserialize } from '.';
import { Joser } from '@scaleforge/joser';
import { faker } from '@faker-js/faker';

describe('serialization', () => {
  const cases = [
    {
      id: new EventId(),
      type: randomBytes(2).readUint16BE(),
      aggregate: {
        id: randomBytes(13),
        version: randomBytes(4).readUint32BE(),
      },
      body: {
        message: faker.lorem.paragraph(),
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
      body: {
        number: 1,
        string: 'string',
        boolean: true,
        null: null,
        Date: new Date(),
        Buffer: randomBytes(128),
        Array: [1, 2, 3],
        Object: {
          Date: new Date(),
          Buffer: randomBytes(128),
          Array: [1, 2, 3],
          Object: {
            Date: new Date(),
            Buffer: randomBytes(128),
            Array: [1, 2, 3],
          },
        },
      },
      meta: {},
      timestamp: new Date(Math.floor(Date.now() / 1000) * 1000),
    },
  ];

  test.each(cases)('serialize and deserialize', (input) => {
    const joser = new Joser();

    const result = deserialize(serialize(input, joser), joser);

    expect(result).toMatchObject(input);
  });
});