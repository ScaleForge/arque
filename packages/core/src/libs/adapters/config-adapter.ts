export type Stream = {
  id: string;
  events: number[];
  context: string | null;
  timestamp: Date;
};

export interface ConfigAdapter {
  init(): Promise<void>;

  saveStream(params: {
    id: string;
    events: number[];
    context?: string | null;
  }): Promise<void>;

  findStreams(event: number): Promise<Pick<Stream, 'id' | 'context'>[]>;

  close(): Promise<void>;
}
