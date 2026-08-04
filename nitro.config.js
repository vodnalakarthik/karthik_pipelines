import { defineNitroConfig } from 'nitro/config';

export default defineNitroConfig({
  modules: ['workflow/nitro'],
  serverAssets: [
    {
      baseName: 'pipeline-data',
      dir: '.',
      pattern: 'master_slugs.txt'
    }
  ],
  vercel: { entryFormat: 'node' },
  routes: {
    '/**': { handler: './src/app.js', format: 'node' }
  }
});
