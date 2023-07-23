import { Event, StreamAdapter, StreamEvent, StreamReceiver } from '@arque/core';
import { Kafka, Producer, logLevel } from 'kafkajs';
import { serialize } from './libs/serialization';
import { debug } from 'debug';
import { randomUUID } from 'crypto';

export type Logger = {
  info: (message: string) => void;
  error: (message: string) => void;
  warn: (message: string) => void;
  verbose: (message: string) => void;
};

const PRODUCER_OPTIONS = {
  allowAutoTopicCreation: true,
  idempotent: false,
  retry: {
    maxRetryTime: 1600,
    factor: 0.5,
    initialRetryTime: 100,
    retries: 16,
    multiplier: 2,
  },
};

export class KafkaStreamAdapter implements StreamAdapter {
  private readonly kafka: Kafka;
  private readonly logger: Logger;

  private producerPromise: Promise<Producer> | null = null;

  private transactionalProducerPromise: Promise<Producer> | null = null;

  constructor(private readonly opts?: {
    prefix?: string;
    brokers?: string[];
    logger?: Logger;
  }) {
    this.kafka = new Kafka({
      brokers: opts?.brokers ?? ['localhost:9092'],
      logLevel: logLevel.ERROR,
    });

    this.logger = opts?.logger ?? {
      info: debug('KafkaStreamAdapter:info'),
      error: debug('KafkaStreamAdapter:error'),
      warn: debug('KafkaStreamAdapter:warn'),
      verbose: debug('KafkaStreamAdapter:verbose'),
    };

    this.transactionalProducer().catch(err => {
      this.logger.error(`transactional producer connection error: error=${err.message}`);
    });

    this.producer().catch(err => {
      this.logger.error(`producer connection error: error=${err.message}`);
    });
  }

  async close(): Promise<void> {
    this.logger.info('closing');

    await Promise.all([
      (async () => {
        if (this.producerPromise) {
          const producer = await this.producerPromise;

          await producer.disconnect();
        }
      })(),
      (async () => {
        if (this.transactionalProducerPromise) {
          const producer = await this.transactionalProducerPromise;

          await producer.disconnect();
        }
      })(),
    ]);

    this.logger.info('closed');
  }

  private async transactionalProducer(): Promise<Producer> {
    if (!this.transactionalProducerPromise) {
      this.transactionalProducerPromise = (async () => {
        this.logger.info('connecting transactional producer');

        const producer = this.kafka.producer({
          ...PRODUCER_OPTIONS,
          transactionalId: `${this.opts.prefix ?? 'arque'}-${randomUUID()}}`,
        });
    
        await producer.connect();

        producer.on(producer.events.DISCONNECT, () => {
          this.transactionalProducerPromise = null;
        });

        this.logger.info('transactional producer connected');

        return producer;
      })().catch(err => {
        this.transactionalProducerPromise = null;

        throw err;
      });
    }

    return this.transactionalProducerPromise;
  }

  private async producer(): Promise<Producer> {
    if (!this.producerPromise) {
      this.producerPromise = (async () => {
        this.logger.info('connecting producer');

        const producer = this.kafka.producer(PRODUCER_OPTIONS);
    
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

  async sendEvents(params: { events: StreamEvent[]; }): Promise<void> {
    const messages = params.events.map(event => {
      return {
        value: serialize(event),
        headers: {
          __ctx: event.meta.__ctx || 'global',
        },
      };
    });

    if (messages.length > 1) {
      const producer = await this.transactionalProducer();

      const transaction = await producer.transaction();

      try {
        await transaction.send({
          topic: `${this.opts.prefix ?? 'arque'}.main`,
          messages,
        });
  
        await transaction.commit();
      } catch (err) {
        await transaction.abort();
  
        throw err;
      }

      return;
    }

    const producer = await this.producer();

    await producer.send({
      topic: `${this.opts.prefix ?? 'arque'}.main`,
      messages,
    });
  }

  receiveEvents(_stream: string, _handler: (event: Event) => Promise<void>): Promise<StreamReceiver> {
    throw new Error('Method not implemented.');
  }
}