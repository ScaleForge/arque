export interface ConfigAdapter {
  init(): Promise<void>;

  saveStream(params: {
    id: string;
    events: number[];
  }): Promise<void>;

  findStreams(event: number): Promise<string[]>;

  close(): Promise<void>;
}
