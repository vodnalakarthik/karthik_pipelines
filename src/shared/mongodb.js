import { MongoClient } from 'mongodb';

const DB_NAME = process.env.MONGO_DB_NAME || 'vayuron';
let clientPromise;

export async function getDatabase() {
  if (!process.env.MONGO_URI) throw new Error('MONGO_URI is required');
  if (!clientPromise) {
    const client = new MongoClient(process.env.MONGO_URI);
    clientPromise = client.connect().catch((error) => {
      clientPromise = undefined;
      throw error;
    });
  }
  const client = await clientPromise;
  return client.db(DB_NAME);
}

export async function ensureIndexes(db) {
  await Promise.all([
    db.collection('jobs').createIndex({ external_id: 1, source: 1 }),
    db.collection('jobs').createIndex({ dedup_key: 1 }, { unique: true }),
    db.collection('jobs').createIndex({ posted_at: -1 }),
    db.collection('jobs').createIndex({ ingested_at: -1 }),
    db.collection('jobs').createIndex({ is_active: 1 }),
    db.collection('jobs').createIndex({ source: 1 }),
    db.collection('ingestion_locks').createIndex({ expires_at: 1 }, { expireAfterSeconds: 0 }),
    db.collection('pipeline_runs').createIndex({ pipeline: 1, started_at: -1 })
  ]);
}

export async function acquirePipelineLock(db, pipeline, runId, ttlMinutes = 180) {
  const now = new Date();
  const expiresAt = new Date(now.getTime() + ttlMinutes * 60_000);
  try {
    const result = await db.collection('ingestion_locks').findOneAndUpdate(
      {
        _id: pipeline,
        $or: [{ expires_at: { $lte: now } }, { run_id: runId }]
      },
      {
        $set: { run_id: runId, acquired_at: now, expires_at: expiresAt }
      },
      { upsert: true, returnDocument: 'after' }
    );
    return result?.run_id === runId;
  } catch (error) {
    if (error.code === 11000) return false;
    throw error;
  }
}

export async function releasePipelineLock(db, pipeline, runId) {
  await db.collection('ingestion_locks').deleteOne({ _id: pipeline, run_id: runId });
}

export async function startPipelineRun(db, pipeline, runId) {
  await db.collection('pipeline_runs').updateOne(
    { _id: runId },
    {
      $setOnInsert: {
        pipeline,
        started_at: new Date(),
        status: 'running'
      }
    },
    { upsert: true }
  );
}

export async function finishPipelineRun(db, runId, status, summary = {}) {
  await db.collection('pipeline_runs').updateOne(
    { _id: runId },
    {
      $set: {
        status,
        summary,
        completed_at: new Date()
      }
    },
    { upsert: true }
  );
}

export async function recordIngestionSummary(db, document) {
  await db.collection('ingestion_runs').insertOne({
    ...document,
    run_at: new Date()
  });
}
