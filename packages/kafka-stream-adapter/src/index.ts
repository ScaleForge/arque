import { StreamAdapter, Subscriber } from '@arque/core';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { deserialize, serialize } from './libs/serialization';
import { murmurHash } from 'ohash';
import { debug } from 'debug';
import { Joser, Serializer } from '@scaleforge/joser';
import assert from 'assert';
import { Event  } from './libs/types';
import { backOff } from 'exponential-backoff';

type Options = {
  prefix: string;
  brokers: string[];
  serializers?: Serializer[];
};

export type KafkaStreamAdapterOptions = Partial<Options>;

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
      logLevel: logLevel.INFO,
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
    opts?: { raw?: true, retry?: {
      maxDelay?: number;
      numOfAttempts?: number;
      retry?: (err: Error) => Promise<boolean>;
    } },
  ): Promise<Subscriber> {
    const topic = `${this.opts.prefix}.${stream}`;

    this.logger.info(`subscribing to topic: topic=${topic}`);

    const { logger, joser } = this;

    const consumer = this.kafka.consumer({
      groupId: topic,
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

    await consumer.subscribe({ topic, fromBeginning: true });

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
          delayFirstAttempt: false,
          jitter: 'full',
          startingDelay: 100,
          timeMultiple: 2,
          maxDelay: opts?.retry?.maxDelay ?? 6400,
          numOfAttempts: opts?.retry?.numOfAttempts ?? 24,
          retry: async (err: Error) => {
            logger.warn(`retrying: error=${err.message}`);

            if (opts?.retry?.retry) {
              return opts.retry.retry(err);
            }

            return true;
          },
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