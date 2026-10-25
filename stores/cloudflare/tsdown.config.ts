import { generateTypes } from '@internal/types-builder';
import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: ['src/index.ts', 'src/kv/index.ts', 'src/do/index.ts'],
  format: ['esm', 'cjs'],
  fixedExtension: false,
  nodeProtocol: 'strip',
  clean: true,
  dts: false,
  treeshake: true,
  sourcemap: true,
  deps: {
    alwaysBundle: ['@internal/core'],
    neverBundle: ['@mastra/core'],
  },
  onSuccess: async () => {
    await generateTypes(process.cwd(), new Set(['@internal/core']));
  },
});
