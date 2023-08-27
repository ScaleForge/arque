export type Stream = {
  name: string;
  events: number[];
};

export interface ConfigAdapter {
  saveStream(params: Stream): Promise<void>;

  getStream(params: {
    name: string;
  }): Promise<Stream | null>;

  listStreams(params: { event: number }): Promise<string[]>;

  close(): Promise<void>;
}
