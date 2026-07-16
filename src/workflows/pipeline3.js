import { getDatabase } from '../shared/mongodb.js';
import { getPipeline3SlugCount, runPipeline3Batch } from '../pipelines/pipeline3.js';
import {
  beginPipelineStep,
  completePipelineStep,
  failPipelineStep,
  releasePipelineStep
} from './shared-steps.js';

async function getSlugCountStep() {
  'use step';
  return getPipeline3SlugCount();
}

async function ingestPipeline3BatchStep(startIndex, batchSize) {
  'use step';
  const db = await getDatabase();
  return runPipeline3Batch(db, startIndex, batchSize);
}

function total(results, key) {
  return results.reduce((sum, result) => sum + Number(result[key] || 0), 0);
}

function pipeline3Summary(results, slugCount) {
  const errors = results.flatMap((result) => result.errors || []);
  return {
    pipeline: 'pipeline3',
    source: 'multi-ats',
    ats_platforms: ['greenhouse', 'lever', 'ashby'],
    slugs_tried: slugCount,
    slugs_found: total(results, 'slugs_found'),
    total_fetched: total(results, 'total_fetched'),
    new_jobs: total(results, 'new_jobs'),
    too_old: total(results, 'too_old'),
    not_us: total(results, 'not_us'),
    filtered: total(results, 'filtered'),
    deduped: total(results, 'deduped'),
    errors: errors.slice(0, 50),
    status: errors.length ? 'partial' : 'success'
  };
}

export async function pipeline3Workflow(input) {
  'use workflow';
  const pipeline = 'pipeline3';
  const acquired = await beginPipelineStep(pipeline, input.runId);
  if (!acquired) return { status: 'skipped', reason: 'already-running' };

  try {
    const slugCount = await getSlugCountStep();
    const batchSize = Math.max(25, Math.min(250, Number(input.batchSize) || 100));
    const results = [];
    for (let startIndex = 0; startIndex < slugCount; startIndex += batchSize) {
      results.push(await ingestPipeline3BatchStep(startIndex, batchSize));
    }
    const summary = pipeline3Summary(results, slugCount);
    await completePipelineStep(input.runId, summary);
    return summary;
  } catch (error) {
    await failPipelineStep(input.runId, error.message);
    throw error;
  } finally {
    await releasePipelineStep(pipeline, input.runId);
  }
}
