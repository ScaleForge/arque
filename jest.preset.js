module.exports = {
  ...require('@nx/jest/preset').default,
  testEnvironment: 'node',
  testTimeout: 30000,
  transform: {
    '^.+\\.(t|j)s$': [
      'ts-jest',
      {
        tsconfig: '<rootDir>/tsconfig.test.json',
        useESM: true,
      },
    ],
  },
  transformIgnorePatterns: ['node_modules/(?!@faker-js).+'],
  moduleFileExtensions: ['ts', 'js'],
};
