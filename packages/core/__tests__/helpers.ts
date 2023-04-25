export function arrayToAsyncIterableIterator<T>(items: T[]): AsyncIterableIterator<T> {
  let index = 0;
  
  return {
    async next() {
      const value = items[index++];

      return {
        value,
        done: value === undefined,
      };
    },
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}