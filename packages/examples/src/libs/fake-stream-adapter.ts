import { StreamAdapter } from '@arque/core';

export class FakeStreamAdapter implements StreamAdapter {
  async subscribe() {
    return {
      stop: async () => {}
    }
  }

  async close() {}

  async sendEvents() {}
}