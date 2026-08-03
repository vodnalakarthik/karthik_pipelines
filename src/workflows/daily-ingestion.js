import { sleep } from 'workflow';
import { start } from 'workflow/api';
import { pipeline1Workflow } from './pipeline1.js';
import { pipeline2Workflow } from './pipeline2.js';
import { pipeline3Workflow } from './pipeline3.js';

async function dispatchPipelinesStep(input) {
  'use step';
  const batchSize = Math.max(25, Math.min(250, Number(process.env.PIPELINE3_BATCH_SIZE) || 100));
  const [pipeline1, pipeline2, pipeline3] = await Promise.all([
    start(pipeline1Workflow, [{ runId: `${input.runId}:pipeline1` }]),
    start(pipeline2Workflow, [{ runId: `${input.runId}:pipeline2` }]),
    start(pipeline3Workflow, [{ runId: `${input.runId}:pipeline3`, batchSize }])
  ]);
  return {
    pipeline1RunId: pipeline1.runId,
    pipeline2RunId: pipeline2.runId,
    pipeline3RunId: pipeline3.runId
  };
}

export async function dailyIngestionWorkflow(input) {
  'use workflow';
  if (input.delaySeconds > 0) await sleep(`${input.delaySeconds}s`);
  return dispatchPipelinesStep(input);
}
