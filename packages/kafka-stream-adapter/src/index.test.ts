import { EventId } from '@arque/core';
import { KafkaStreamAdapter } from '.';
import { Event } from './libs/types';

const mockSendBatch = jest.fn().mockResolvedValue(undefined);

jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    producer: () => ({
      connect: jest.fn().mockResolvedValue(undefined),
      sendBatch: mockSendBatch,
    }),
  })),
  logLevel: { INFO: 4 },
}));

describe('KafkaStreamAdapter.sendEvents', () => {
  const defaultContext = Buffer.from('default');
  const namedContext = Buffer.from('named');
  const contexts = { __: defaultContext, platform: namedContext };

  function event(ctx?: Event['meta']['__ctx']): Event {
    return {
      id: new EventId(),
      type: 1,
      aggregate: { id: Buffer.alloc(13), version: 1 },
      body: null,
      meta: ctx === undefined ? {} : { __ctx: ctx },
      timestamp: new Date(),
    };
  }

  beforeEach(() => mockSendBatch.mockClear());

  test.each<{
    name: string;
    stream: string | { id: string; context: string | null };
    ctx: Event['meta']['__ctx'];
    expected: Buffer;
  }>([
    { name: 'named context', stream: { id: 'main', context: 'platform' }, ctx: contexts, expected: namedContext },
    { name: 'missing named context', stream: { id: 'main', context: 'missing' }, ctx: contexts, expected: defaultContext },
    { name: 'null stream context', stream: { id: 'main', context: null }, ctx: contexts, expected: defaultContext },
    { name: 'empty stream context', stream: { id: 'main', context: '' }, ctx: contexts, expected: defaultContext },
    { name: 'string stream', stream: 'main', ctx: contexts, expected: defaultContext },
    { name: 'plain Buffer context', stream: { id: 'main', context: 'platform' }, ctx: namedContext, expected: namedContext },
    { name: 'plain Buffer with string stream', stream: 'main', ctx: namedContext, expected: namedContext },
    { name: 'absent context', stream: { id: 'main', context: 'platform' }, ctx: undefined, expected: Buffer.from([0]) },
    { name: 'empty named Buffer', stream: { id: 'main', context: 'platform' }, ctx: { __: defaultContext, platform: Buffer.alloc(0) }, expected: Buffer.alloc(0) },
  ])('$name', async ({ stream, ctx, expected }) => {
    const adapter = new KafkaStreamAdapter();
    const input = event(ctx);

    await adapter.sendEvents([{ stream, events: [input] }]);

    expect(mockSendBatch).toHaveBeenCalledWith({
      topicMessages: [{
        topic: 'arque.main',
        messages: [{ value: expect.any(Buffer), headers: { __ctx: expected } }],
      }],
    });
    expect(input.meta.__ctx).toBe(ctx);
  });

  test('selects context independently for each event and stream', async () => {
    const adapter = new KafkaStreamAdapter();

    await adapter.sendEvents([
      { stream: { id: 'first', context: 'platform' }, events: [event(contexts), event(defaultContext)] },
      { stream: { id: 'second', context: null }, events: [event(contexts)] },
    ]);

    expect(mockSendBatch).toHaveBeenCalledWith({
      topicMessages: [
        { topic: 'arque.first', messages: [
          { value: expect.any(Buffer), headers: { __ctx: namedContext } },
          { value: expect.any(Buffer), headers: { __ctx: defaultContext } },
        ] },
        { topic: 'arque.second', messages: [
          { value: expect.any(Buffer), headers: { __ctx: defaultContext } },
        ] },
      ],
    });
  });
});
