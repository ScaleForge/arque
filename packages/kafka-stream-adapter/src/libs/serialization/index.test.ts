import { EventId } from '@arque/core';
import { randomBytes } from 'crypto';
import { serialize, deserialize } from '.';
import { Joser } from '@scaleforge/joser';
import { faker } from '@faker-js/faker';

describe('serialize', () => {
  const cases = [
    {
      name: 'text body with buffer context',
      input: {
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
    },
    {
      name: 'nested body with empty metadata',
      input: {
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
    },
    {
      name: 'null body with buffer context',
      input: {
        id: new EventId(),
        type: randomBytes(2).readUint16BE(),
        aggregate: {
          id: randomBytes(13),
          version: randomBytes(4).readUint32BE(),
        },
        body: null,
        meta: {
          __ctx: randomBytes(13),
        },
        timestamp: new Date(Math.floor(Date.now() / 1000) * 1000),
      },
    },
    {
      name: 'object context with buffer fields',
      input: {
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
          __ctx: {
            __: randomBytes(13),
            platform: randomBytes(13),
          },
        },
        timestamp: new Date(Math.floor(Date.now() / 1000) * 1000),
      },
    },
  ];

  test.each(cases.map(({ name, input }) => ({ name, input })))('serialize and deserialize: $name', ({ input }) => {
    const joser = new Joser();

    const result = deserialize(serialize(input as Parameters<typeof serialize>[0], joser), joser);

    expect(result).toMatchObject(input);
  });
});
