import { getDatabase } from '../shared/mongodb.js';
import { PIPELINE2_ROLE_COUNT, runPipeline2Role } from '../pipelines/pipeline2.js';
import {
  beginPipelineStep,
  completePipelineStep,
  failPipelineStep,
  releasePipelineStep
} from './shared-steps.js';

async function ingestPipeline2RoleStep(roleIndex) {
  'use step';
  const db = await getDatabase();
  return runPipeline2Role(db, roleIndex);
}

function total(results, select) {
  return results.reduce((sum, result) => sum + Number(select(result) || 0), 0);
}

function pipeline2Summary(results) {
  const errors = results.flatMap((result) => [
    ...(result.jsearch.errors || []),
    ...(result.techmap.errors || [])
  ]);
  return {
    pipeline: 'pipeline2',
    roles: results.map((result) => ({
      role: result.role,
      jsearch: result.jsearch,
      techmap: result.techmap,
      duration_seconds: result.duration_seconds
    })),
    total_new_jobs: total(results, (result) => result.jsearch.saved + result.techmap.saved),
    total_deduped: total(results, (result) => result.jsearch.deduped + result.techmap.deduped),
    total_filtered: total(results, (result) => result.jsearch.filtered + result.techmap.filtered),
    total_not_us: total(results, (result) => result.jsearch.not_us + result.techmap.not_us),
    errors: errors.slice(0, 50),
    status: errors.length ? 'partial' : 'success'
  };
}

export async function pipeline2Workflow(input) {
  'use workflow';
  const pipeline = 'pipeline2';
  const acquired = await beginPipelineStep(pipeline, input.runId);
  if (!acquired) return { status: 'skipped', reason: 'already-running' };

  try {
    const results = [];
    for (let roleIndex = 0; roleIndex < PIPELINE2_ROLE_COUNT; roleIndex += 1) {
      results.push(await ingestPipeline2RoleStep(roleIndex));
    }
    const summary = pipeline2Summary(results);
    await completePipelineStep(input.runId, summary);
    return summary;
  } catch (error) {
    await failPipelineStep(input.runId, error.message);
    throw error;
  } finally {
    await releasePipelineStep(pipeline, input.runId);
  }
}
