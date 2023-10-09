export interface ConfigAdapter {
  saveStream(params: {
    id: string;
    events: number[];
  }): Promise<void>;

  findStreams(event: number): Promise<string[]>;

  close(): Promise<void>;
}
