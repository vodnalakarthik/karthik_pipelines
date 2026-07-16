import { getDatabase } from '../shared/mongodb.js';
import { runPipeline1 } from '../pipelines/pipeline1.js';
import {
  beginPipelineStep,
  completePipelineStep,
  failPipelineStep,
  releasePipelineStep
} from './shared-steps.js';

async function ingestPipeline1Step() {
  'use step';
  const db = await getDatabase();
  return runPipeline1(db);
}

export async function pipeline1Workflow(input) {
  'use workflow';
  const pipeline = 'pipeline1';
  const acquired = await beginPipelineStep(pipeline, input.runId);
  if (!acquired) return { status: 'skipped', reason: 'already-running' };

  try {
    const summary = await ingestPipeline1Step();
    await completePipelineStep(input.runId, summary);
    return summary;
  } catch (error) {
    await failPipelineStep(input.runId, error.message);
    throw error;
  } finally {
    await releasePipelineStep(pipeline, input.runId);
  }
}
