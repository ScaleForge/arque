export type Stream = {
  id: string;
  events: number[];
};

export interface ConfigAdapter {
  saveStream(params: Stream): Promise<void>;

  findStream(params: {
    id: string;
  }): Promise<Stream | null>;

  findStreams(params: { event: number }): Promise<Stream[]>;

  close(): Promise<void>;
}
