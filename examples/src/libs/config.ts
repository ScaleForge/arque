import { faker } from '@faker-js/faker';

export const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092,localhost:9093,localhost:9094').split(',');

export const MONGODB_URI = process.env.MONGODB_URI
  ?? `mongodb://mongo1:27021,mongo2:27022,mongo3:27023/${faker.string.alphanumeric(6)}?replicaSet=rs0`;
