export class AggregateVersionConflictError extends Error {
  constructor(id: Buffer, version: number) {
    super(
      `aggregate version conflict: id=${id.toString('hex')} version=${version}`
    );
  }
}