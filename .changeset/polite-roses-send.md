---
'@mastra/cloudflare': minor
---

Added Observational Memory support to Durable Objects storage (`CloudflareDOStorage` from `@mastra/cloudflare/do`). Agents that keep memory in a Durable Object can now turn on `observationalMemory`, including async buffering, and keep observations next to the thread's messages in the same object.

```typescript
import { Memory } from '@mastra/memory';
import { CloudflareDOStorage } from '@mastra/cloudflare/do';

const memory = new Memory({
  storage: new CloudflareDOStorage({ sql: ctx.storage.sql }),
  options: {
    observationalMemory: { model: 'openai/gpt-6-luna' },
  },
});
```

`init()` creates the `mastra_observational_memory` table, so existing Durable Objects get it the next time storage initializes. Resource-scoped message listing (`listMessagesByResourceId`) now works with Durable Objects storage too.
