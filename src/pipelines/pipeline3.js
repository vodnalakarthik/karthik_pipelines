// pipeline3.js
// Vayuron Job Ingestion — Pipeline 3
// Sources: Greenhouse + Lever + Ashby (direct public APIs, no Apify needed)
// For each slug in master_slugs.txt, tries all 3 ATS APIs in parallel
// Run: node pipeline3.js

import "dotenv/config";
import { useStorage } from "nitro/storage";

// ─── CONFIG ───────────────────────────────────────────────

// How many companies to process in parallel (be respectful)
const CONCURRENCY = 5;
// Delay between batches (ms)
const BATCH_DELAY = 500;

// ─── LOAD SLUGS ───────────────────────────────────────────

export async function loadSlugs() {
  const contents = await useStorage("assets:pipeline-data").getItemRaw("master_slugs.txt");
  if (contents == null) throw new Error("Bundled server asset master_slugs.txt is missing");
  const lines = String(contents).split("\n");
  const slugs = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    // Format: "1\tdatadog" or just "datadog"
    const parts = trimmed.split(/\s+/);
    const slug = parts[parts.length - 1].toLowerCase();
    if (slug) slugs.push(slug);
  }
  return slugs;
}

// ─── NON-US LOCATION BLOCKLIST ────────────────────────────

const NON_US = [
  "india", "canada", "uk", "united kingdom", "australia", "germany",
  "france", "netherlands", "singapore", "brazil", "mexico", "poland",
  "ireland", "spain", "italy", "sweden", "switzerland", "israel",
  "japan", "china", "south korea", "new zealand", "bangalore",
  "bengaluru", "mumbai", "delhi", "hyderabad", "pune", "toronto",
  "vancouver", "london", "berlin", "amsterdam", "paris", "sydney",
  "dubai", "apac", "emea", "latam", "europe", "asia",
];

function isUSLocation(location) {
  // Whitelist-only — must have explicit US signal, reject everything else
  if (!location) return false;
  const l = location.toLowerCase();
  if (l.includes("united states") || l.includes(" usa") || l.includes(", usa")) return true;

  // US state abbreviation pattern e.g. "New York, NY" or ", TX,"
  const usStates = ["al","ak","az","ar","ca","co","ct","de","fl","ga",
    "hi","id","il","in","ia","ks","ky","la","me","md","ma","mi","mn",
    "ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
    "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy"];
  const parts = l.split(/[,\s]+/);
  if (parts.some(p => usStates.includes(p))) return true;

  return false; // no explicit US signal = reject
}

// ─── 24 HOUR FILTER ──────────────────────────────────────
// Only keep jobs posted in the last 24 hours
// Each ATS uses a different date field — handled in normalizer
// Greenhouse: updated_at | Lever: createdAt | Ashby: publishedAt

function isPostedToday(postedAt) {
  if (!postedAt) return true; // no date = assume recent, let dedup handle
  const posted = new Date(postedAt);
  if (isNaN(posted.getTime())) return true; // invalid date = skip filter
  // Compute fresh each call so the cutoff is accurate even in long-running cron
  const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
  return posted >= cutoff;
}

// ─── TARGET TITLES FILTER ─────────────────────────────────

const TARGET_TITLES = [
  "data engineer", "analytics engineer", "data platform", "etl engineer",
  "data analyst", "business analyst", "bi analyst", "business intelligence",
  "data scientist", "applied scientist", "research scientist",
  "software engineer", "software developer", "backend engineer",
  "frontend engineer", "full stack", "fullstack", "platform engineer",
  "staff engineer", "machine learning engineer", "ml engineer",
  "ai engineer", "llm engineer", "mlops", "deep learning",
  "cloud engineer", "devops engineer", "site reliability", "sre",
  "infrastructure engineer", "data infrastructure",
];

function isTitleRelevant(title) {
  if (!title) return false;
  const t = title.toLowerCase();
  return TARGET_TITLES.some(kw => t.includes(kw));
}

// ─── STAFFING AGENCIES ────────────────────────────────────

const STAFFING = [
  "robert half", "randstad", "teksystems", "insight global",
  "kforce", "apex systems", "cybercoders", "kelly services",
  "manpower", "adecco", "staffmark", "beacon hill", "judge group",
  "mastech", "infosys bpm", "wipro", "cognizant", "mindtree",
  "syntel", "luxoft", "epam systems", "modis", "experis",
  "talently", "scale.jobs", "trabajo.org",
  "aaratech", "weekday", "goliath partners", "jack & jill",
  "watch learn apply", "sundayy", "hired", "ladders",
];

const BLOCKED_JOB_PUBLISHERS = [
  "digitalhire", "talently", "scale.jobs", "trabajo.org",
  "aaratech", "weekday", "goliath partners", "jack & jill",
  "watch learn apply", "sundayy", "hired", "ladders",
];

// ─── US LOCATION WHITELIST ────────────────────────────────
// Only accept jobs with explicit US signals — reject everything else
const US_STATES = ["al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy"];

function isUSJob(job) {
  const country = (job.job_country || "").toUpperCase().trim();

  // Explicit country field — trust it completely
  if (country === "US" || country === "USA") return true;
  if (country && country !== "US" && country !== "USA") return false;

  // No country — check location string for US signals
  const loc = (job.location || "").toLowerCase();
  if (loc.includes("united states")) return true;
  if (loc.includes(", usa") || loc.includes("usa,") || loc === "usa") return true;

  // "Remote" with no other location context = US assumed (US-based job boards)
  if ((loc === "remote" || loc === "remote, ") && !job.job_state) return true;

  // US state in dedicated state field
  const state = (job.job_state || "").toLowerCase().trim();
  if (state && US_STATES.includes(state)) return true;

  // US state abbreviation in location string e.g. "Austin, TX" → "tx"
  const locParts = loc.split(/[,\s]+/).map(s => s.trim()).filter(Boolean);
  if (locParts.some(p => US_STATES.includes(p))) return true;

  return false; // no explicit US signal = reject
}

function isStaffingAgency(company) {
  if (!company) return false;
  const c = company.toLowerCase();
  return STAFFING.some(a => c.includes(a));
}

function isBlockedJobPublisher(jobPublisher) {
  if (!jobPublisher) return false;
  const publisher = jobPublisher.toLowerCase().trim();
  return BLOCKED_JOB_PUBLISHERS.some(blocked => publisher.includes(blocked));
}

// ─── DEDUPLICATION ────────────────────────────────────────

function buildDedupKey(company, title, date) {
  const c = (company || "").toLowerCase().trim().replace(/\s+/g, "-");
  const t = (title   || "").toLowerCase().trim().replace(/\s+/g, "-");
  const parsedDate = date ? new Date(date) : null;
  const d = parsedDate && !isNaN(parsedDate.getTime())
    ? parsedDate.toISOString().split("T")[0]
    : "unknown";
  return `${c}|${t}|${d}`;
}

async function isDuplicate(db, job) {
  // dedup_key (company + title + date) is the single source of truth
  // external_id is optional info only — not used for dedup
  if (!job.dedup_key) return false;
  const match = await db.collection("jobs").findOne({ dedup_key: job.dedup_key });
  return !!match;
}

// ─── GREENHOUSE FETCHER ───────────────────────────────────

async function fetchGreenhouse(slug) {
  try {
    const res = await fetch(
      `https://boards-api.greenhouse.io/v1/boards/${slug}/jobs?content=true`,
      { signal: AbortSignal.timeout(15000) }
    );
    if (!res.ok) return [];
    const data = await res.json();
    return (data.jobs || []).map(j => ({
      external_id:     String(j.id),
      source:          "greenhouse",
      company_name:    slug,
      title:           j.title || "",
      description:     (j.content || "").replace(/<[^>]+>/g, "").trim(),
      location:        j.location?.name || "",
      department:      j.departments?.[0]?.name || "",
      apply_url:       j.absolute_url || "",
      is_direct_apply: true,
      job_publisher:   "Greenhouse",
      posted_at:       j.updated_at ? new Date(j.updated_at) : new Date(),
      dedup_key:       buildDedupKey(slug, j.title, j.updated_at),
      ingested_at:     new Date(),
      is_active:       true,
    }));
  } catch { return []; }
}

// ─── LEVER FETCHER ────────────────────────────────────────

async function fetchLever(slug) {
  try {
    const res = await fetch(
      `https://api.lever.co/v0/postings/${slug}?mode=json`,
      { signal: AbortSignal.timeout(15000) }
    );
    if (!res.ok) return [];
    const data = await res.json();
    const jobs = Array.isArray(data) ? data : (data.postings || []);
    return jobs.map(j => ({
      external_id:     j.id || "",
      source:          "lever",
      company_name:    slug,
      title:           j.text || "",
      description:     j.descriptionPlain || j.description || "",
      location:        j.categories?.location || j.workplaceType || "",
      department:      j.categories?.department || j.categories?.team || "",
      apply_url:       j.hostedUrl || j.applyUrl || "",
      is_direct_apply: true,
      job_publisher:   "Lever",
      posted_at:       j.createdAt ? new Date(j.createdAt) : new Date(),
      dedup_key:       buildDedupKey(slug, j.text, j.createdAt),
      ingested_at:     new Date(),
      is_active:       true,
    }));
  } catch { return []; }
}

// ─── ASHBY FETCHER ────────────────────────────────────────

async function fetchAshby(slug) {
  try {
    const res = await fetch(
      `https://api.ashbyhq.com/posting-api/job-board/${slug}`,
      { signal: AbortSignal.timeout(15000) }
    );
    if (!res.ok) return [];
    const data = await res.json();
    const jobs = data.jobs || data.jobPostings || [];
    return jobs.map(j => ({
      external_id:     j.id || j.jobId || "",
      source:          "ashby",
      company_name:    slug,
      title:           j.title || j.jobTitle || "",
      description:     j.descriptionPlain || j.jobDescriptionPlain || "",
      location:        j.location || j.locationName || j.workplaceType || "",
      department:      j.department || j.team || "",
      apply_url:       j.jobUrl || j.applyUrl || "",
      is_direct_apply: true,
      job_publisher:   "Ashby",
      posted_at:       j.publishedAt ? new Date(j.publishedAt) : new Date(),
      dedup_key:       buildDedupKey(slug, j.title || j.jobTitle, j.publishedAt),
      ingested_at:     new Date(),
      is_active:       true,
    }));
  } catch { return []; }
}

// ─── PROCESS ONE SLUG ─────────────────────────────────────
// Try all 3 ATS APIs for each slug
// Most slugs only exist on one — the others return []

async function processSlug(slug, db, stats) {
  // Fetch from all 3 in parallel
  const [ghJobs, lvJobs, ashJobs] = await Promise.all([
    fetchGreenhouse(slug),
    fetchLever(slug),
    fetchAshby(slug),
  ]);

  const allJobs = [...ghJobs, ...lvJobs, ...ashJobs];
  if (!allJobs.length) return;

  // 24hr filter — applied before any other processing
  const recentJobs = allJobs.filter(job => isPostedToday(job.posted_at));
  stats.too_old += (allJobs.length - recentJobs.length);
  if (!recentJobs.length) return;

  stats.found++;

  for (const job of recentJobs) {
    stats.fetched++;

    // Title filter
    if (!isTitleRelevant(job.title)) { stats.filtered++; continue; }

    // US location filter
    if (!isUSLocation(job.location)) { stats.not_us++; continue; }

    // Staffing filter
    if (isStaffingAgency(job.company_name)) { stats.filtered++; continue; }
    if (isBlockedJobPublisher(job.job_publisher)) { stats.filtered++; continue; }

    // Quality check — must have title, apply_url, and description >= 100 chars
    if (!job.title || !job.apply_url || (job.description || "").trim().length < 100) {
      stats.filtered++; continue;
    }

    // US filter
    if (!isUSJob(job)) { stats.not_us++; continue; }

    // Dedup
    if (await isDuplicate(db, job)) { stats.deduped++; continue; }

    // Save
    try {
      await db.collection("jobs").insertOne(job);
      stats.saved++;
      const src = job.source.padEnd(10);
      console.log(`  ✅ [${src}] ${job.title.slice(0,50)} @ ${slug}`);
    } catch (e) {
      if (e.code === 11000) { stats.deduped++; }
      else { stats.errors.push(`${slug}: ${e.message}`); }
    }
  }
}

// ─── BATCH PROCESSOR ──────────────────────────────────────

async function processBatch(slugs, db, stats) {
  for (let i = 0; i < slugs.length; i += CONCURRENCY) {
    const batch = slugs.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(slug => processSlug(slug, db, stats)));
    if (i + CONCURRENCY < slugs.length) {
      await new Promise(r => setTimeout(r, BATCH_DELAY));
    }
    // Progress every 100 slugs
    if ((i + CONCURRENCY) % 100 === 0) {
      console.log(`  📊 Progress: ${Math.min(i + CONCURRENCY, slugs.length)}/${slugs.length} slugs | saved: ${stats.saved} | found: ${stats.found}`);
    }
  }
}

// ─── MAIN INGESTION ───────────────────────────────────────

export async function runPipeline3Batch(db, startIndex = 0, batchSize = 100) {
  const start = Date.now();
  const allSlugs = await loadSlugs();
  const slugs = allSlugs.slice(startIndex, startIndex + batchSize);

  console.log(`\n${"═".repeat(56)}`);
  console.log(`  MULTI-ATS INGESTION — ${new Date().toISOString()}`);
  console.log(`  Slugs: ${slugs.length} | ATS: Greenhouse + Lever + Ashby`);
  console.log(`  Concurrency: ${CONCURRENCY} | Batch delay: ${BATCH_DELAY}ms`);
  console.log(`${"═".repeat(56)}\n`);

  const stats = {
    fetched: 0, saved: 0, filtered: 0,
    deduped: 0, not_us: 0, found: 0,
    too_old: 0, errors: [],
  };

  await processBatch(slugs, db, stats);

  const duration = Math.round((Date.now() - start) / 1000);

  console.log(`\n${"═".repeat(56)}`);
  console.log(`  Slugs tried    : ${slugs.length}`);
  console.log(`  Slugs with jobs: ${stats.found}`);
  console.log(`  Total fetched  : ${stats.fetched}`);
  console.log(`  New jobs saved : ${stats.saved}`);
  console.log(`  Too old (>24h) : ${stats.too_old}`);
  console.log(`  Not US         : ${stats.not_us}`);
  console.log(`  Filtered       : ${stats.filtered}`);
  console.log(`  Deduped        : ${stats.deduped}`);
  console.log(`  Errors         : ${stats.errors.length}`);
  console.log(`  Duration       : ${Math.floor(duration/60)}m ${duration%60}s`);
  console.log(`  Status         : ${stats.errors.length === 0 ? "✅ success" : "⚠️ partial"}`);
  console.log(`${"═".repeat(56)}\n`);

  return {
    start_index: startIndex,
    slugs_tried: slugs.length,
    slugs_found: stats.found,
    total_fetched: stats.fetched,
    new_jobs: stats.saved,
    too_old: stats.too_old,
    not_us: stats.not_us,
    filtered: stats.filtered,
    deduped: stats.deduped,
    errors: stats.errors.slice(0, 50),
    duration_seconds: duration,
    status: stats.errors.length === 0 ? "success" : "partial"
  };
}

// ─── ENTRY POINT ──────────────────────────────────────────

export async function getPipeline3SlugCount() {
  return (await loadSlugs()).length;
}
