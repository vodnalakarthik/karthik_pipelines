import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { start } from 'workflow/api';
import { dailyIngestionWorkflow } from './workflows/daily-ingestion.js';

const app = express();
app.disable('etag');
app.use(express.json({ limit: '100kb' }));

function chicagoHour(date = new Date()) {
  const value = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Chicago',
    hour: '2-digit',
    hour12: false
  }).format(date);
  return Number(value);
}

export function delayUntilFiveChicago(date = new Date()) {
  const hour = chicagoHour(date);
  if (hour === 5) return 0;
  if (hour === 4) return 60 * 60;
  throw new Error(`Cron invoked outside its expected 4–5 AM America/Chicago window (hour=${hour})`);
}

function isAuthorized(req) {
  return Boolean(
    process.env.CRON_SECRET &&
    req.get('authorization') === `Bearer ${process.env.CRON_SECRET}`
  );
}

app.get('/api/health', (_req, res) => {
  res.json({ ok: true, service: 'karthik-pipelines' });
});

app.get('/api/cron/daily-ingestion', async (req, res, next) => {
  try {
    if (!isAuthorized(req)) return res.status(401).json({ message: 'Unauthorized' });

    const force = req.query.force === 'true' && process.env.NODE_ENV !== 'production';
    const delaySeconds = force ? 0 : delayUntilFiveChicago();
    const runId = `daily:${new Date().toISOString().slice(0, 10)}:${randomUUID()}`;
    const run = await start(dailyIngestionWorkflow, [{ runId, delaySeconds }]);

    return res.status(202).json({
      accepted: true,
      coordinatorRunId: run.runId,
      scheduledRunId: runId,
      delaySeconds
    });
  } catch (error) {
    return next(error);
  }
});

app.use((req, res) => {
  res.status(404).json({ message: `Route not found: ${req.method} ${req.originalUrl}` });
});

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(500).json({ message: error.message || 'Unexpected server error' });
});

export default app;
