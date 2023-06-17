import { randomBytes } from 'crypto';
import { Event, EventId } from '@arque/core';
import { faker } from '@faker-js/faker';
import { Joser } from '@scaleforge/joser';

const joser = new Joser();

export function generateFakeEvent(): Event {
  return {
    id: new EventId(),
    type: 0,
    aggregate: {
      id: randomBytes(13),
      version: 1,
    },
    body: joser.serialize({
      Buffer: randomBytes(8),
      Date: new Date(),
      Set: new Set([1, 2, 3]),
      Map: new Map([
        ['a', 1],
        ['b', 2],
        ['c', 3],
      ]),
      number: faker.datatype.number(),
      string: faker.datatype.string(),
      boolean: faker.datatype.boolean(),
      null: null,
    }),
    meta: {},
    timestamp: new Date(),
  };
}
