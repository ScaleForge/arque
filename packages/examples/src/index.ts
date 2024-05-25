import { select } from '@clack/prompts';
import { execute as executeBasicAggregate } from './basic-aggregate'

enum Action {
    BasicAggregate,
    Snapshot,
    Projection,
  }

async function main() {
  const action = await select({
    message: 'Which example do you want to execute?',
    options: [
        {
        value: Action.BasicAggregate,
        label: 'basic aggregate',
        },
        {
        value: Action.Snapshot,
        label: 'snapshot',
        },
    ],
  });

  if (action === Action.BasicAggregate) {
    await executeBasicAggregate();
  }
}

main();
