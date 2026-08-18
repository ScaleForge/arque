import eslint from '@eslint/js';
import stylistic from '@stylistic/eslint-plugin';
import typescriptParser from '@typescript-eslint/parser';
import typescript from '@typescript-eslint/eslint-plugin';
import globals from 'globals';

export default [
  {
    ignores: ['node_modules/**/*', '**/dist/**/*', '.nx/**/*'],
  },
  eslint.configs.recommended,
  {
    files: ['**/*.{js,mjs,ts}'],
    languageOptions: {
      globals: globals.node,
    },
    plugins: {
      '@stylistic': stylistic,
    },
    rules: {
      semi: 'off',
      '@stylistic/semi': ['error', 'always'],
      'comma-dangle': 'off',
      '@stylistic/comma-dangle': ['error', 'always-multiline'],
      'object-curly-spacing': 'off',
      '@stylistic/object-curly-spacing': ['error', 'always'],
      quotes: 'off',
      '@stylistic/quotes': ['error', 'single'],
    },
  },
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser: typescriptParser,
      parserOptions: {
        tsconfigRootDir: import.meta.dirname,
        project: [
          './tsconfig.json',
          './packages/*/tsconfig.json',
          './packages/*/tsconfig.build.json',
          './packages/*/tsconfig.test.json',
        ],
      },
    },
    plugins: {
      '@typescript-eslint': typescript,
    },
    rules: {
      ...typescript.configs.recommended.rules,
      'no-unused-vars': 'off',
      '@typescript-eslint/no-var-requires': 'off',
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_.*$' },
      ],
    },
  },
  {
    files: ['**/*.test.ts', '**/jest.config.ts'],
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.jest,
      },
    },
  },
];
