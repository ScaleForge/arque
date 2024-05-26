import { StreamAdapter } from '@arque/core';

export class FakeStreamAdapter implements StreamAdapter {
  async init() {}

  async subscribe() {
    return {
      stop: async () => {}
    }
  }

  async sendEvents() {}

  async close() {}
}