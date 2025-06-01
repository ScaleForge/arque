export class AggregateError extends Error {
  constructor(readonly code: string, message: string, opts?: { cause?: unknown }) {
    super(message, opts);
  }
}
