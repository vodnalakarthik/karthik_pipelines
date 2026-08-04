// pipeline2.js
// Vayuron Job Ingestion — Pipeline 2
// Sources: JSearch + Techmap → MongoDB
// Run: node pipeline2.js

import "dotenv/config";

// ─── CONFIG ───────────────────────────────────────────────

const JSEARCH_API_KEY  = process.env.JSEARCH_API_KEY;
const TECHMAP_API_KEY  = process.env.TECHMAP_API_KEY;

// ─── QUERIES (7 roles — same across both sources) ─────────

const JSEARCH_QUERIES = [
  "data engineer in United States",
  "data analyst in United States",
  "data scientist in United States",
  "software engineer in United States",
  "machine learning engineer in United States",
  "AI engineer in United States",
  "devops engineer in United States",
];

const TECHMAP_SEARCHES = [
  { title: "data engineer",             label: "Data Engineer" },
  { title: "data analyst",              label: "Data Analyst" },
  { title: "data scientist",            label: "Data Scientist" },
  { title: "software engineer",         label: "Software Engineer" },
  { title: "machine learning engineer", label: "ML Engineer" },
  { title: "AI engineer",               label: "AI Engineer" },
  { title: "devops engineer",           label: "DevOps Engineer" },
];

// Techmap: 18 pages × 10 jobs × 7 queries = 1,260 jobs/day
const TECHMAP_MAX_PAGES = 18;  // max safe: 3,000 req ÷ 23 days ÷ 7 roles = 18
const JSEARCH_MAX_PAGES = 62;  // max safe: 10,000 req ÷ 23 days ÷ 7 roles = 62 (stops early if no more results)

// ─── STAFFING AGENCIES ────────────────────────────────────

const STAFFING_AGENCIES = [
  "robert half", "randstad", "teksystems", "insight global",
  "kforce", "apex systems", "cybercoders", "kelly services",
  "manpower", "adecco", "staffmark", "beacon hill", "judge group",
  "mastech", "infosys bpm", "wipro", "cognizant", "mindtree",
  "syntel", "luxoft", "epam systems", "modis", "experis",
  // Job portal re-posters (post jobs under their own brand)
  "talently", "scale.jobs", "trabajo.org",
  "aaratech", "weekday", "goliath partners", "jack & jill",
  "watch learn apply", "sundayy", "hired", "ladders",
];

const BLOCKED_JOB_PUBLISHERS = [
  "digitalhire", "talently", "scale.jobs", "trabajo.org",
  "aaratech", "weekday", "goliath partners", "jack & jill",
  "watch learn apply", "sundayy", "hired", "ladders",
];

// ─── SHARED FILTERS (local side: only these 3) ────────────

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

function isStaffingAgency(companyName) {
  if (!companyName) return false;
  const c = companyName.toLowerCase();
  return STAFFING_AGENCIES.some((agency) => c.includes(agency));
}

function isBlockedJobPublisher(jobPublisher) {
  if (!jobPublisher) return false;
  const publisher = jobPublisher.toLowerCase().trim();
  return BLOCKED_JOB_PUBLISHERS.some((blocked) => publisher.includes(blocked));
}

function isQualityJob(job) {
  if (!job.title) return false;
  if (!job.description || job.description.trim().length < 100) return false;
  if (!job.apply_url) return false;
  return true;
}

function buildDedupKey(companyName, title, postedDate) {
  const c = (companyName || "").toLowerCase().trim().replace(/\s+/g, "-");
  const t = (title || "").toLowerCase().trim().replace(/\s+/g, "-");
  const parsedDate = postedDate ? new Date(postedDate) : null;
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

// ─── JSEARCH ──────────────────────────────────────────────

async function fetchJSearchJobs(query) {
  const allJobs = [];

  for (let page = 1; page <= JSEARCH_MAX_PAGES; page++) {
    const url = new URL("https://jsearch.p.rapidapi.com/search-v2");
    url.searchParams.set("query", query);
    // JSearch's date_posted enum doesn't have an exact "48h" option
    // (only today/3days/week/month) — "3days" is the closest fit that
    // still guarantees a full 48h window is covered.
    url.searchParams.set("date_posted", "3days");
    url.searchParams.set("num_pages", "1");
    url.searchParams.set("page", String(page));
    url.searchParams.set("employment_types", "FULLTIME");
    url.searchParams.set("country", "us");

    const response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        "x-rapidapi-key": JSEARCH_API_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com",
      },
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`JSearch ${response.status} for: "${query}" page ${page}: ${errorBody}`);
    }

    const data = await response.json();
    const pageJobs = data.data || [];

    if (pageJobs.length === 0) {
      console.log(`     Stopped at page ${page} — no more results`);
      break;
    }

    allJobs.push(...pageJobs);

    if (page < JSEARCH_MAX_PAGES) {
      await new Promise((r) => setTimeout(r, 300));
    }
  }

  return allJobs;
}

function normalizeJSearchJob(raw, query) {
  const isDirectApply = raw.job_apply_is_direct || false;
  return {
    external_id: raw.job_id,
    source: "jsearch",
    search_query: query,
    company_name: raw.employer_name || "Unknown",
    company_logo: raw.employer_logo || "",
    title: raw.job_title || "",
    description: raw.job_description || "",
    employment_type: raw.job_employment_type || "FULLTIME",
    apply_url: raw.job_apply_link || "",
    is_direct_apply: isDirectApply,
    direct_apply_url: isDirectApply ? raw.job_apply_link : null,
    publisher_url: !isDirectApply ? raw.job_apply_link : null,
    job_publisher: raw.job_publisher || "",
    location: `${raw.job_city || ""}, ${raw.job_state || ""}`.trim(),
    job_city: raw.job_city || "",
    job_state: raw.job_state || "",
    job_country: raw.job_country || "US",
    is_remote: raw.job_is_remote || false,
    is_usa: true,
    salary_min: raw.job_min_salary || null,
    salary_max: raw.job_max_salary || null,
    salary_currency: raw.job_salary_currency || "USD",
    salary_period: raw.job_salary_period || null,
    required_skills: raw.job_required_skills || [],
    dedup_key: buildDedupKey(raw.employer_name, raw.job_title, raw.job_posted_at_datetime_utc),
    posted_at: raw.job_posted_at_datetime_utc ? new Date(raw.job_posted_at_datetime_utc) : new Date(),
    ingested_at: new Date(),
    is_active: true,
  };
}

async function runJSearch(db, queries = JSEARCH_QUERIES) {
  console.log("\n━━━ JSearch ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  const stats = { fetched: 0, saved: 0, filtered: 0, deduped: 0, not_us: 0, errors: [] };

  for (const query of queries) {
    console.log(`\n  🔍 "${query}"`);
    try {
      const rawJobs = await fetchJSearchJobs(query);
      stats.fetched += rawJobs.length;
      console.log(`     Fetched : ${rawJobs.length} jobs`);

      for (const raw of rawJobs) {
        const job = normalizeJSearchJob(raw, query);
        if (isStaffingAgency(job.company_name)) { stats.filtered++; continue; }
        if (isBlockedJobPublisher(job.job_publisher)) { stats.filtered++; continue; }
        if (!isQualityJob(job)) { stats.filtered++; continue; }
        if (!isUSJob(job)) { stats.not_us++; continue; }
        if (await isDuplicate(db, job)) { stats.deduped++; continue; }
        try {
          await db.collection("jobs").insertOne(job);
          stats.saved++;
          const type = job.is_direct_apply ? "🏢" : "🔗";
          console.log(`     ${type} ${job.title} @ ${job.company_name} — ${job.job_city}, ${job.job_state}`);
        } catch (e) {
          if (e.code === 11000) { stats.deduped++; }
          else { stats.errors.push(e.message); }
        }
      }

      await new Promise((r) => setTimeout(r, 1000));
    } catch (err) {
      console.error(`     ❌ ${err.message}`);
      stats.errors.push(err.message);
    }
  }

  console.log(`\n  JSearch → Fetched: ${stats.fetched} | Saved: ${stats.saved} | Filtered: ${stats.filtered} | Deduped: ${stats.deduped}`);
  return stats;
}

// ─── TECHMAP ──────────────────────────────────────────────

async function fetchTechmapPages(titleKeyword) {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1); // yesterday + today = last 24 hours
  const dateMaxStr = today.toISOString().split("T")[0];
  const dateMinStr = yesterday.toISOString().split("T")[0];
  const allJobs = [];
  let totalCount = 0;

  for (let page = 1; page <= TECHMAP_MAX_PAGES; page++) {
    const url = new URL("https://daily-international-job-postings.p.rapidapi.com/api/v2/jobs/search");
    url.searchParams.set("title", titleKeyword);
    url.searchParams.set("countryCode", "us");
    url.searchParams.set("dateCreatedMin", dateMinStr);
    url.searchParams.set("dateCreatedMax", dateMaxStr);
    url.searchParams.set("language", "en");
    url.searchParams.set("workType", "fulltime");
    url.searchParams.set("page", String(page));
    url.searchParams.set("format", "json");

    const response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "x-rapidapi-host": "daily-international-job-postings.p.rapidapi.com",
        "x-rapidapi-key": TECHMAP_API_KEY,
      },
    });

    if (!response.ok) {
      const err = await response.text();
      throw new Error(`Techmap ${response.status}: ${err}`);
    }

    const data = await response.json();
    totalCount = data.totalCount || 0;
    const pageJobs = data.result || [];

    if (pageJobs.length === 0) break;
    allJobs.push(...pageJobs);

    if (page < TECHMAP_MAX_PAGES && pageJobs.length === 10) {
      await new Promise((r) => setTimeout(r, 1500));
    }
  }

  return { total: totalCount, jobs: allJobs };
}

function normalizeTechmapJob(raw, searchLabel) {
  const jld = raw.jsonLD || {};
  const salary = jld.baseSalary?.value || {};
  const applyUrl = jld.url || "";
  const isDirect = raw.isDirect || false;

  return {
    external_id: jld.identifier || raw._id || "",
    source: "techmap",
    search_query: searchLabel,
    company_name: raw.company || jld.hiringOrganization?.name || "Unknown",
    company_logo: jld.image || "",
    title: raw.title || jld.title || "",
    description: jld.description || "",
    employment_type: raw.workType?.[0] || "FULLTIME",
    occupation: raw.occupation || "",
    career_level: raw.careerLevel?.[0] || null,
    apply_url: applyUrl,
    is_direct_apply: isDirect,
    direct_apply_url: isDirect ? applyUrl : null,
    publisher_url: !isDirect ? applyUrl : null,
    job_publisher: raw.portal || "techmap",
    location: `${raw.city || ""}, ${raw.state || ""}`.trim(),
    job_city: raw.city || "",
    job_state: raw.state || "",
    job_country: "US",
    is_remote: (raw.workPlace || []).some((w) => w.toLowerCase().includes("remote")),
    is_usa: true,
    salary_min: raw.minSalary ? Number(raw.minSalary) : (salary.minValue ? Number(salary.minValue) : null),
    salary_max: raw.maxSalary ? Number(raw.maxSalary) : (salary.maxValue ? Number(salary.maxValue) : null),
    salary_currency: jld.salaryCurrency || "USD",
    salary_period: salary.unitText || null,
    required_skills: raw.skills || [],
    dedup_key: buildDedupKey(
      raw.company || jld.hiringOrganization?.name,
      raw.title || jld.title,
      raw.dateCreated
    ),
    posted_at: raw.dateCreated ? new Date(raw.dateCreated) : new Date(),
    ingested_at: new Date(),
    is_active: true,
  };
}

async function runTechmap(db, searches = TECHMAP_SEARCHES) {
  console.log("\n━━━ Techmap ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  const stats = { fetched: 0, saved: 0, filtered: 0, deduped: 0, not_us: 0, errors: [] };

  for (const search of searches) {
    console.log(`\n  🔍 "${search.title}" (max ${TECHMAP_MAX_PAGES} pages)`);
    try {
      const { total, jobs } = await fetchTechmapPages(search.title);
      stats.fetched += jobs.length;
      console.log(`     Available: ${total} | Fetched: ${jobs.length}`);

      for (const raw of jobs) {
        if (raw.isDuplicate) { stats.filtered++; continue; }

        const job = normalizeTechmapJob(raw, search.label);
        if (isStaffingAgency(job.company_name)) { stats.filtered++; continue; }
        if (isBlockedJobPublisher(job.job_publisher)) { stats.filtered++; continue; }
        if (!isQualityJob(job)) { stats.filtered++; continue; }
        if (!isUSJob(job)) { stats.not_us++; continue; }
        if (await isDuplicate(db, job)) { stats.deduped++; continue; }

        try {
          await db.collection("jobs").insertOne(job);
          stats.saved++;
          const salaryStr = job.salary_min ? ` 💰 $${Number(job.salary_min).toLocaleString()}` : "";
          console.log(`     ➕ ${job.title} @ ${job.company_name} — ${job.job_city}, ${job.job_state}${salaryStr}`);
        } catch (e) {
          if (e.code === 11000) { stats.deduped++; }
          else { stats.errors.push(e.message); }
        }
      }

      await new Promise((r) => setTimeout(r, 2000));
    } catch (err) {
      console.error(`     ❌ ${err.message}`);
      stats.errors.push({ query: search.title, error: err.message });
    }
  }

  console.log(`\n  Techmap → Fetched: ${stats.fetched} | Saved: ${stats.saved} | Filtered: ${stats.filtered} | Deduped: ${stats.deduped}`);
  return stats;
}

// ─── MAIN PIPELINE ────────────────────────────────────────

export async function runPipeline2(db) {
  if (!JSEARCH_API_KEY) throw new Error("JSEARCH_API_KEY missing");
  if (!TECHMAP_API_KEY) throw new Error("TECHMAP_API_KEY missing");
  const start = Date.now();
  console.log(`\n${"═".repeat(52)}`);
  console.log(`  VAYURON PIPELINE 2 — ${new Date().toISOString()}`);
  console.log(`  Sources: JSearch + Techmap`);
  console.log(`${"═".repeat(52)}`);

  const jsearchStats = await runJSearch(db);
  const techmapStats = await runTechmap(db);

  const totalSaved    = jsearchStats.saved   + techmapStats.saved;
  const totalDeduped  = jsearchStats.deduped + techmapStats.deduped;
  const totalFiltered = jsearchStats.filtered + techmapStats.filtered;
  const duration = Math.round((Date.now() - start) / 1000);
  const hasErrors = jsearchStats.errors.length + techmapStats.errors.length > 0;

  console.log(`\n${"═".repeat(52)}`);
  console.log(`  JSearch saved    : ${jsearchStats.saved}`);
  console.log(`  Techmap saved    : ${techmapStats.saved}`);
  console.log(`  ────────────────────────────────────`);
  console.log(`  Total new jobs   : ${totalSaved}`);
  console.log(`  Deduped          : ${totalDeduped}`);
  console.log(`  Filtered         : ${totalFiltered}`);
  console.log(`  Duration         : ${Math.floor(duration / 60)}m ${duration % 60}s`);
  console.log(`  Status           : ${hasErrors ? "⚠️  partial" : "✅ success"}`);
  console.log(`${"═".repeat(52)}\n`);

  await db.collection("ingestion_runs").insertOne({
    pipeline: "pipeline2",
    run_at: new Date(),
    jsearch: jsearchStats,
    techmap: techmapStats,
    total_new_jobs: totalSaved,
    total_deduped: totalDeduped,
    total_filtered: totalFiltered,
    duration_seconds: duration,
    status: hasErrors ? "partial" : "success",
  });
}

// ─── ENTRY POINT ──────────────────────────────────────────

export const PIPELINE2_ROLE_COUNT = JSEARCH_QUERIES.length;

export async function runPipeline2Role(db, roleIndex) {
  if (!JSEARCH_API_KEY) throw new Error("JSEARCH_API_KEY missing");
  if (!TECHMAP_API_KEY) throw new Error("TECHMAP_API_KEY missing");
  const query = JSEARCH_QUERIES[roleIndex];
  const search = TECHMAP_SEARCHES[roleIndex];
  if (!query || !search) throw new Error(`Invalid Pipeline 2 role index: ${roleIndex}`);

  const startedAt = Date.now();
  const [jsearch, techmap] = await Promise.all([
    runJSearch(db, [query]),
    runTechmap(db, [search])
  ]);

  return {
    roleIndex,
    role: search.label,
    jsearch,
    techmap,
    duration_seconds: Math.round((Date.now() - startedAt) / 1000)
  };
}
