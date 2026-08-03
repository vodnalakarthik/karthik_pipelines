import {
  acquirePipelineLock,
  ensureIndexes,
  finishPipelineRun,
  getDatabase,
  recordIngestionSummary,
  releasePipelineLock,
  startPipelineRun
} from '../shared/mongodb.js';

export async function beginPipelineStep(pipeline, runId) {
  'use step';
  const db = await getDatabase();
  await ensureIndexes(db);
  const acquired = await acquirePipelineLock(db, pipeline, runId);
  if (acquired) await startPipelineRun(db, pipeline, runId);
  return acquired;
}

export async function completePipelineStep(runId, summary) {
  'use step';
  const db = await getDatabase();
  await finishPipelineRun(db, runId, summary.status || 'success', summary);
  await recordIngestionSummary(db, summary);
}

export async function failPipelineStep(runId, error) {
  'use step';
  const db = await getDatabase();
  await finishPipelineRun(db, runId, 'failed', { error });
}

export async function releasePipelineStep(pipeline, runId) {
  'use step';
  const db = await getDatabase();
  await releasePipelineLock(db, pipeline, runId);
}
