import { randomBytes } from 'crypto';
import { Event, EventId } from '@arque/core';
import { faker } from '@faker-js/faker';

export function generateEvent(): Event {
  return {
    id: EventId.generate(),
    type: 0,
    aggregate: {
      id: randomBytes(13),
      version: 1,
    },
    body: {
      Buffer: randomBytes(8),
      Date: new Date(),
      Set: new Set([1, 2, 3]),
      Map: new Map([
        ['a', 1],
        ['b', 2],
        ['c', 3],
      ]),
      number: faker.number.int(),
      string: faker.string.alphanumeric(32),
      boolean: faker.datatype.boolean(),
      null: null,
    },
    meta: {},
    timestamp: new Date(),
  };
}
