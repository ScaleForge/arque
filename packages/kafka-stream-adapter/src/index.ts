import { StreamAdapter, Subscriber } from '@arque/core';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { deserialize, serialize } from './libs/serialization';
import { murmurHash } from 'ohash';
import { debug } from 'debug';
import { Joser, Serializer } from '@scaleforge/joser';
import assert from 'assert';
import { Event  } from './libs/types';
import { backOff, BackoffOptions } from 'exponential-backoff';

type Options = {
  prefix: string;
  brokers: string[];
  serializers?: Serializer[];
};

export class KafkaStreamAdapter implements StreamAdapter {
  private readonly logger = {
    info: debug('KafkaStreamAdapter:info'),
    error: debug('KafkaStreamAdapter:error'),
    warn: debug('KafkaStreamAdapter:warn'),
    verbose: debug('KafkaStreamAdapter:verbose'),
  };

  private readonly kafka: Kafka;

  private readonly opts: Options;

  private readonly joser: Joser;

  private producerPromise: Promise<Producer> | null = null;

  constructor(opts?: Partial<Options>) {
    this.opts = {
      prefix: opts?.prefix ?? 'arque',
      brokers: opts?.brokers ?? ['localhost:9092'],
    };

    this.kafka = new Kafka({
      brokers: this.opts.brokers,
      logLevel: logLevel.ERROR,
    });

    this.joser = new Joser({
      serializers: opts?.serializers ?? [],
    });

    this.producer().catch(err => {
      this.logger.error(`producer connection error: error=${err.message}`);
    });
  }
  
  async subscribe(
    stream: string,
    handle: (event: Event) => Promise<void>,
    opts?: { raw?: true, retry?: BackoffOptions },
  ): Promise<Subscriber> {
    const { logger, joser } = this;

    const consumer = this.kafka.consumer({
      groupId: `${this.opts.prefix}.${stream}`,
      allowAutoTopicCreation: true,
      retry: {
        maxRetryTime: 1600,
        factor: 0.5,
        initialRetryTime: 100,
        retries: 10,
        multiplier: 2,
      },
    });

    await consumer.connect();

    await consumer.subscribe({ topic: `${this.opts.prefix}.${stream}`, fromBeginning: true });

    consumer.on('consumer.group_join', (event) => {
      this.logger.verbose(`consumer.group_join: ${JSON.stringify(event)}`);
    });
    
    await consumer.run({
      async eachMessage({ message }) {
        assert(message.value, '`message.value` is null');

        const event = deserialize(message.value!, joser, opts?.raw);

        logger.verbose(
          `event received: ${JSON.stringify({
            id: event.id.toString(),
            type: event.type,
            aggregate: {
              id: event.aggregate.id.toString('hex'),
              version: event.aggregate.version,
            },
          })}`
        );

        await backOff(async () => {
          await handle(event);
        }, {
          delayFirstAttempt: opts?.retry?.delayFirstAttempt ?? false,
          jitter: opts?.retry?.jitter ?? 'full',
          maxDelay: opts?.retry?.maxDelay ?? 6400,
          numOfAttempts: opts?.retry?.numOfAttempts ?? 24,
          startingDelay: opts?.retry?.startingDelay ?? 100,
          timeMultiple: opts?.retry?.timeMultiple ?? 2,
          retry: opts?.retry?.retry,
        });
      },
    });

    return {
      stop: async () => {
        await consumer.disconnect();
      },
    };
  }

  private async producer(): Promise<Producer> {
    if (!this.producerPromise) {
      this.producerPromise = (async () => {
        this.logger.info('connecting producer');

        const producer = this.kafka.producer({
          allowAutoTopicCreation: true,
          idempotent: false,
          retry: {
            maxRetryTime: 1600,
            factor: 0.5,
            initialRetryTime: 100,
            retries: 10,
            multiplier: 2,
          },
          createPartitioner: () => ({ partitionMetadata, message }) =>
            murmurHash(message.headers.__ctx as Buffer) % partitionMetadata.length,
        });
    
        await producer.connect();

        producer.on(producer.events.DISCONNECT, () => {
          this.producerPromise = null;
        });

        this.logger.info('producer connected');

        return producer;
      })().catch(err => {
        this.producerPromise = null;

        throw err;
      });
    }

    return this.producerPromise;
  }

  async sendEvents(
    params: {
      stream: string;
      events: Event[];
    }[],
    opts?: { raw?: true },
  ): Promise<void> {
    const producer = await this.producer();

    await producer.sendBatch({
      topicMessages: params.map(({ stream, events }) => {
        return {
          topic: `${this.opts.prefix}.${stream}`,
          messages: events.map(event => {
            return {
              value: serialize(event, this.joser, opts?.raw),
              headers: {
                __ctx: event.meta.__ctx ?? Buffer.from([0]),
              },
            };
          }),
        };
      }),
    });
  }

  async close(): Promise<void> {
    this.logger.info('closing');

    if (this.producerPromise) {
      const producer = await this.producerPromise;

      await producer.disconnect();
    }

    this.logger.info('closed');
  }
}