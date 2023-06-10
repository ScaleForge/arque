/* eslint-disable @typescript-eslint/no-var-requires */
const nxPreset = require('@nrwl/jest/preset').default;

module.exports = {
  ...nxPreset,
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
