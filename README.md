# Karthik Pipelines

Durable job-ingestion workflows for Vayuron. One Vercel Cron invocation starts three independent workflows that write normalized jobs to the shared `vayuron.jobs` MongoDB collection.

## Architecture

```text
Vercel Cron (weekdays at 16:30 UTC)
  -> GET /api/cron/daily-ingestion
  -> dailyIngestionWorkflow
       -> wait until 11:30 AM America/Chicago when necessary
       -> Pipeline 1 workflow: Apify LinkedIn + Fantastic Jobs
       -> Pipeline 2 workflow: JSearch + Techmap, one durable step per role
       -> Pipeline 3 workflow: Greenhouse + Lever + Ashby, batched slug steps
  -> MongoDB Atlas: vayuron.jobs
```

The cron endpoint returns `202` after starting the coordinator. The ingestion work continues durably in Workflow steps.

## Why the work is split

- Pipeline 1 normally completes in a few minutes and runs as one bounded ingestion step.
- Pipeline 2 previously took 30–55 minutes. It now runs seven role steps so no single function must contain the entire pipeline.
- Pipeline 3 tries thousands of company slugs. It now processes configurable batches, defaulting to 100 slugs per durable step.
- Each child workflow has its own MongoDB lock, run status, error handling, and final ingestion summary.

## Schedule and daylight saving time

Vercel Cron uses UTC. The single schedule is:

```text
30 16 * * 1-5
```

At 16:30 UTC it is either 11:30 AM CDT or 10:30 AM CST. During standard time the coordinator uses a durable one-hour sleep before starting the pipelines. This keeps execution at 11:30 AM America/Chicago year-round while retaining one cron definition.

## Collections

- `jobs`: normalized jobs shared with the Vayuron portal.
- `ingestion_runs`: final summaries compatible with the existing ingestion history.
- `pipeline_runs`: durable operational status for each child pipeline run.
- `ingestion_locks`: expiring locks that prevent overlapping or duplicated pipeline executions.

The application creates indexes idempotently. It never drops indexes during scheduled runs.

## Environment variables

Copy `.env.example` to `.env` for local development:

```bash
cp .env.example .env
```

Required:

```text
MONGO_URI
APIFY_API_KEY
FANTASTIC_JOBS_API_KEY
JSEARCH_API_KEY
TECHMAP_API_KEY
CRON_SECRET
```

Optional:

```text
MONGO_DB_NAME=vayuron
PIPELINE3_BATCH_SIZE=100
```

Never commit `.env`. Add the same values to the Vercel project's Production environment when deployment is configured.

## Local development

```bash
npm install
npm run dev
```

Health check:

```text
GET http://localhost:5050/api/health
```

Start a local coordinator run outside the 10:30–11:30 AM scheduling window:

```bash
curl -H "Authorization: Bearer $CRON_SECRET" \
  "http://localhost:5050/api/cron/daily-ingestion?force=true"
```

The local Workflow runtime writes state to `.workflow-data/`, which is ignored by Git.

## Validation

```bash
npm run check
npm run build
```

These checks do not call source APIs or write to MongoDB.

## Later Vercel setup

1. Push this repository to its private GitHub repository.
2. Import the repository as a new Vercel project.
3. Keep the framework preset as Other.
4. Add all Production environment variables.
5. Enable Fluid Compute.
6. Deploy the production branch.
7. Confirm `/api/health` responds.
8. Confirm the Cron Jobs page lists `/api/cron/daily-ingestion`.
9. Review Workflow and Function logs after the first run.

Only production deployments receive Vercel Cron invocations.

## Security and reliability

- The cron endpoint requires `Authorization: Bearer <CRON_SECRET>`.
- API keys and MongoDB credentials remain server-side.
- Unique `dedup_key` enforcement prevents duplicate job insertion across sources.
- Pipeline locks protect against duplicate cron delivery and overlapping manual runs.
- Workflow steps retry transient failures without restarting already completed steps.
- Each pipeline can fail independently without preventing the other pipelines from starting.
