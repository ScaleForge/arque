import { EventId } from './event-id';
import R from 'ramda';

describe('EventId', () => {
  test('generate unique ids', () => {
    const COUNT = 1000000;
    const ids = new Set(R.times(() => new EventId().toString('hex'), COUNT));
    expect(ids.size).toBe(COUNT);
  });

  test('generate ordered ids', () => {
    const COUNT = 1000000;

    const ids = R.times(() => new EventId().toString('hex'), COUNT);

    expect(ids).toEqual(ids.slice().sort());
  });
});
