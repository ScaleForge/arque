module.exports = {
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/recommended',
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    tsconfigRootDir: __dirname,
    project: ['./tsconfig.json', './tsconfig.eslint.json', './packages/*/tsconfig.json', './packages/*/tsconfig.build.json', './packages/*/tsconfig.test.json'],
  },
  plugins: ['@typescript-eslint'],
  env: {
    node: true,
  },
  root: true,
  ignorePatterns: ['node_modules/**/*'],
  rules: {
    'semi': 'off',
    '@typescript-eslint/semi': [
      'error',
      'always',
    ],
    'comma-dangle': 'off',
    '@typescript-eslint/comma-dangle': [
      'error',
      'always-multiline',
    ],
    'object-curly-spacing': 'off',
    '@typescript-eslint/object-curly-spacing': [
      'error',
      'always',
    ],
    'quotes': 'off',
    '@typescript-eslint/quotes': [
      'error',
      'single',
    ],
  },
};
