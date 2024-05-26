import { StreamAdapter, Subscriber } from '@arque/core';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { deserialize, serialize } from './libs/serialization';
import { murmurHash } from 'ohash';
import { debug } from 'debug';
import { Joser, Serializer } from '@scaleforge/joser';
import { Event  } from './libs/types';
import { backOff } from 'exponential-backoff';
import assert from 'assert';

type Options = {
  prefix: string;
  brokers: string[];
  serializers?: Serializer[];
};

export type KafkaStreamAdapterOptions = Partial<Options>;

export class KafkaStreamAdapter implements StreamAdapter {
  private readonly logger = {
    info: debug('info:KafkaStreamAdapter'),
    error: debug('error:KafkaStreamAdapter'),
    warn: debug('warn:KafkaStreamAdapter'),
    verbose: debug('verbose:KafkaStreamAdapter'),
  };

  private readonly kafka: Kafka;

  private readonly opts: Options;

  private readonly joser: Joser;

  private _producer: Promise<Producer>;

  private _init: Promise<void>;

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

  public async init() {
    if (!this._init) {
      this._init = (async () => {
        await this.producer();
      })().catch((err) => {
        delete this._init;

        throw err;
      });
    }

    await this._init;
  }

  private async producer(): Promise<Producer> {
    if (!this._producer) {
      this._producer = (async () => {
        const producer = this.kafka.producer({
          allowAutoTopicCreation: true,
          idempotent: false,
          retry: {
            maxRetryTime: 800,
            factor: 0.5,
            initialRetryTime: 100,
            retries: 5,
            multiplier: 2,
          },
          createPartitioner: () => ({ partitionMetadata, message }) =>
            murmurHash(message.headers.__ctx as Buffer) % partitionMetadata.length,
        });
    
        await producer.connect();

        return producer;
      })().catch((err) => {
        delete this._producer;

        throw err;
      });
    }

    return this._producer;
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
    if (this._producer) {
      const producer = await this._producer;

      await producer.disconnect();
    }
  }
}