/* eslint-disable @typescript-eslint/no-var-requires */
module.exports = {
  ...require('@nx/jest/preset').default,
  testEnvironment: 'node',
  testTimeout: 30000,
  transform: {
    '^.+\\.ts$': [
      'ts-jest',
      {
        tsconfig: '<rootDir>/tsconfig.test.json',
      },
    ],
  },
  moduleFileExtensions: ['ts', 'js'],
};
