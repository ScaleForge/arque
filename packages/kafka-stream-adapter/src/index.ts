import { Event, StreamAdapter, Subscriber } from '@arque/core';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { serialize } from './libs/serialization';
import { murmurHash } from 'ohash';
import { debug } from 'debug';
import { Joser, Serializer } from '@scaleforge/joser';

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
  
  subscribe(_params: { stream: string; handle: (event: Event) => Promise<void>; }): Promise<Subscriber> {
    throw new Error('not implemented');
  }

  private async producer(): Promise<Producer> {
    if (!this.producerPromise) {
      this.producerPromise = (async () => {
        this.logger.info('connecting producer');

        const producer = this.kafka.producer({
          allowAutoTopicCreation: false,
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

  async sendEvents(events: Event[], stream: string, ctx?: Buffer): Promise<void> {
    const messages = events.map(event => {
      return {
        value: serialize(event, this.joser),
        headers: {
          __ctx: ctx ?? Buffer.from([0]),
        },
      };
    });

    const producer = await this.producer();

    await producer.send({
      topic: `${this.opts.prefix}.${stream}`,
      messages,
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