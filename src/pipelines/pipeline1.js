// pipeline1.js
// Vayuron Job Ingestion — Pipeline 1
// Sources: Apify LinkedIn + Fantastic Jobs → MongoDB
// Run: node pipeline1.js

import "dotenv/config";
import fetch from "node-fetch";

// ─── CONFIG ───────────────────────────────────────────────

const APIFY_API_KEY      = process.env.APIFY_API_KEY;
const FANTASTIC_API_KEY  = process.env.FANTASTIC_JOBS_API_KEY;

// ─── QUERIES (7 roles — same across both sources) ─────────

const LINKEDIN_COUNT_PER_ROLE = 142;  // 142 × 7 roles = ~1,000 total/day = ~$1.00/day

const LINKEDIN_URLS = [
  "https://www.linkedin.com/jobs/search/?keywords=data+engineer&location=United+States&f_TPR=r172800&f_JT=F",
  "https://www.linkedin.com/jobs/search/?keywords=data+analyst&location=United+States&f_TPR=r172800&f_JT=F",
  "https://www.linkedin.com/jobs/search/?keywords=data+scientist&location=United+States&f_TPR=r172800&f_JT=F",
  "https://www.linkedin.com/jobs/search/?keywords=software+engineer&location=United+States&f_TPR=r172800&f_JT=F",
  "https://www.linkedin.com/jobs/search/?keywords=machine+learning+engineer&location=United+States&f_TPR=r172800&f_JT=F",
  "https://www.linkedin.com/jobs/search/?keywords=AI+engineer&location=United+States&f_TPR=r172800&f_JT=F",
  "https://www.linkedin.com/jobs/search/?keywords=devops+engineer&location=United+States&f_TPR=r172800&f_JT=F",
];

const FANTASTIC_ROLES = [
  "Data Engineer",
  "Data Analyst",
  "Data Scientist",
  "AI Engineer",
  "Machine Learning Engineer",
  "Software Engineer",
  "Devops Engineer",
];

const FANTASTIC_TIME_FRAME = "24h";
// NOTE: Fantastic.jobs' time_frame only supports 1h/24h/7d/6m — no 48h option.
// Kept at 24h to conserve the limited monthly job quota (user decision — see chat).
// This means Fantastic has a smaller lookback window than the other 3 sources,
// which are all set to a true 48h window.
const FANTASTIC_LIMIT = 124; // max safe: 20,000 jobs ÷ 23 days ÷ 7 roles = 124
const FANTASTIC_LOCATION = "united states";  // Fantastic API location filter
// NOTE: Fantastic.jobs' active-ats endpoint does not have a clearly documented
// employment_type filter param. Full-time filtering for this source currently
// relies on quality/description checks only — flagging as a known gap.

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

// ─── APIFY LINKEDIN ───────────────────────────────────────

async function fetchApifyJobs() {
  const runRes = await fetch(
    `https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/runs?token=${APIFY_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        urls: LINKEDIN_URLS,
        count: LINKEDIN_COUNT_PER_ROLE,
        maxItems: LINKEDIN_COUNT_PER_ROLE * LINKEDIN_URLS.length, // total cap = ~1,000
        scrapeCompanyDetails: true,
        proxy: { useApifyProxy: true },
      }),
    }
  );

  if (!runRes.ok) {
    const errText = await runRes.text();
    throw new Error(`Apify start failed: ${runRes.status} — ${errText}`);
  }

  const runData = await runRes.json();
  const runId = runData.data.id;
  const datasetId = runData.data.defaultDatasetId;
  console.log(`  ▶ Apify started. Run ID: ${runId}`);

  let status = "RUNNING";
  let attempts = 0;
  while (status === "RUNNING" && attempts < 60) {
    await new Promise((r) => setTimeout(r, 10000));
    attempts++;
    const statusRes = await fetch(`https://api.apify.com/v2/actor-runs/${runId}?token=${APIFY_API_KEY}`);
    const statusData = await statusRes.json();
    status = statusData.data.status;
    console.log(`  ⏳ ${status} (${attempts * 10}s)`);
  }

  if (status !== "SUCCEEDED") throw new Error(`Apify did not succeed. Status: ${status}`);

  const resultsRes = await fetch(
    `https://api.apify.com/v2/datasets/${datasetId}/items?format=json&clean=true&token=${APIFY_API_KEY}`
  );
  if (!resultsRes.ok) throw new Error(`Apify results fetch failed: ${resultsRes.status}`);
  return await resultsRes.json();
}

function normalizeApifyJob(raw) {
  const title = raw.title || "";
  const company = raw.companyName || "Unknown";
  const location = raw.location || "";
  const parts = location.split(",").map((p) => p.trim());
  const city = parts[0] || "";
  const state = parts[1] || "";
  const linkedinUrl = raw.link || "";
  // applyUrl is always empty from this actor — use linkedin job URL as apply URL
  const applyUrl = linkedinUrl;
  const isDirectApply = false;

  return {
    external_id: raw.id || raw.trackingId || "",
    source: "apify_linkedin",
    search_query: raw.inputUrl || "",
    company_name: company,
    company_logo: raw.companyLogo || "",
    company_website: raw.companyWebsite || null,
    company_linkedin: raw.companyLinkedinUrl || null,
    company_employees: raw.companyEmployeesCount || null,
    title: title,
    description: raw.descriptionText || "",
    description_html: raw.descriptionHtml || "",
    employment_type: raw.employmentType || "FULLTIME",
    seniority_level: raw.seniorityLevel || null,
    job_function: raw.jobFunction || null,
    industries: raw.industries || [],
    apply_url: applyUrl,
    is_direct_apply: isDirectApply,
    is_easy_apply: false, // actor doesn't return applyUrl — using linkedin job URL
    direct_apply_url: null,
    publisher_url: linkedinUrl,
    job_publisher: "LinkedIn",
    location: location,
    job_city: city,
    job_state: state,
    job_country: "US",
    is_remote: location.toLowerCase().includes("remote"),
    is_usa: true,
    salary: raw.salary || null,
    salary_min: null,
    salary_max: null,
    salary_currency: "USD",
    required_skills: [],
    applicant_count: raw.applicantsCount || null,
    dedup_key: buildDedupKey(company, title, raw.postedAt),
    posted_at: (() => {
      if (!raw.postedAt) return new Date();
      const d = new Date(raw.postedAt);
      return isNaN(d.getTime()) ? new Date() : d; // handles "2 days ago" gracefully
    })(),
    ingested_at: new Date(),
    is_active: true,
  };
}

async function runApify(db) {
  console.log("\n━━━ Apify LinkedIn ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  const stats = { fetched: 0, saved: 0, filtered: 0, deduped: 0, not_us: 0, errors: [] };

  try {
    const rawJobs = await fetchApifyJobs();
    stats.fetched = rawJobs.length;
    console.log(`  ✅ Returned: ${rawJobs.length} jobs`);

    for (const raw of rawJobs) {
      const job = normalizeApifyJob(raw);

      if (isStaffingAgency(job.company_name)) { stats.filtered++; continue; }
      if (isBlockedJobPublisher(job.job_publisher)) { stats.filtered++; continue; }
      if (!isQualityJob(job)) { stats.filtered++; continue; }
      if (await isDuplicate(db, job)) { stats.deduped++; continue; }
      try {
        await db.collection("jobs").insertOne(job);
        stats.saved++;
        const type = job.is_direct_apply ? "🏢" : "🔗";
        console.log(`  ${type} ${job.title} @ ${job.company_name} — ${job.job_city}, ${job.job_state}`);
      } catch (e) {
        if (e.code === 11000) { stats.deduped++; }
        else { stats.errors.push(e.message); }
      }
    }
  } catch (err) {
    console.error(`  ❌ ${err.message}`);
    stats.errors.push(err.message);
  }

  console.log(`\n  Apify → Fetched: ${stats.fetched} | Saved: ${stats.saved} | Filtered: ${stats.filtered} | Deduped: ${stats.deduped}`);
  return stats;
}

// ─── FANTASTIC JOBS ───────────────────────────────────────

async function fetchFantasticJobs(role) {
  const url = `https://data.fantastic.jobs/v1/active-ats?time_frame=${FANTASTIC_TIME_FRAME}&limit=${FANTASTIC_LIMIT}&title=${encodeURIComponent(role)}&location=${encodeURIComponent(FANTASTIC_LOCATION)}&description_format=text`;

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${FANTASTIC_API_KEY}` },
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Fantastic ${res.status} for "${role}": ${errText}`);
  }

  const data = await res.json();
  return Array.isArray(data) ? data : (data && data.jobs) || [];
}

function normalizeFantasticJob(raw, role) {
  const place = raw.locations?.[0]?.address || {};
  const city = place.addressLocality || "";
  const state = place.addressRegion || "";
  const country = place.addressCountry || "";
  const jobPublisher = raw.job_publisher || raw.publisher || raw.source || raw.portal || raw.provider || "fantastic-jobs";

  return {
    external_id: String(raw.id || raw.url || ""),
    source: "fantastic-jobs",
    search_query: role,
    company_name: raw.organization || "Unknown",
    company_website: raw.organization_url || null,
    title: raw.title || "",
    description: raw.description || raw.descriptionText || raw.description_text || raw.body || raw.text || "",
    employment_type: "FULLTIME",
    apply_url: raw.url || raw.organization_url || "",
    is_direct_apply: true, // Fantastic sources directly from ATS/career sites
    direct_apply_url: raw.url || null,
    publisher_url: null,
    job_publisher: jobPublisher,
    location: [city, state].filter(Boolean).join(", "),
    job_city: city,
    job_state: state,
    job_country: country,
    is_remote: (raw.title || "").toLowerCase().includes("remote") ||
               [city, state].join(" ").toLowerCase().includes("remote"),
    // Only accept explicit US signals — reject everything else
    is_usa: (() => {
      if (country === "US" || country === "USA") return true;
      if (country && country !== "US" && country !== "USA") return false;
      // No country field — check location string for US signals
      const loc = [city, state].join(" ").toLowerCase();
      if (loc.includes("united states") || loc.includes(" usa")) return true;
      // US state abbreviations
      const usStates = ["al","ak","az","ar","ca","co","ct","de","fl","ga",
        "hi","id","il","in","ia","ks","ky","la","me","md","ma","mi","mn",
        "ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok",
        "or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy"];
      if (usStates.includes(state.toLowerCase())) return true;
      return false;
    })(),
    salary_min: null,
    salary_max: null,
    salary_currency: "USD",
    required_skills: [],
    dedup_key: buildDedupKey(raw.organization, raw.title, raw.date_posted),
    posted_at: raw.date_posted ? new Date(raw.date_posted) : new Date(),
    ingested_at: new Date(),
    is_active: true,
  };
}

async function runFantastic(db) {
  console.log("\n━━━ Fantastic Jobs ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  const stats = { fetched: 0, saved: 0, filtered: 0, deduped: 0, not_us: 0, errors: [] };

  for (const role of FANTASTIC_ROLES) {
    console.log(`\n  🔍 "${role}"`);
    try {
      const rawJobs = await fetchFantasticJobs(role);
      stats.fetched += rawJobs.length;
      console.log(`     Fetched : ${rawJobs.length} jobs`);

      for (const raw of rawJobs) {
        const job = normalizeFantasticJob(raw, role);
        // US filter — reject jobs from India and other non-US locations
        if (!job.is_usa) { stats.not_us++; continue; }
        if (isStaffingAgency(job.company_name)) { stats.filtered++; continue; }
        if (isBlockedJobPublisher(job.job_publisher)) { stats.filtered++; continue; }
        // Fantastic: require title and apply_url only
        if (!job.title || !job.apply_url) { stats.filtered++; continue; }
        if (!isUSJob(job)) { stats.not_us++; continue; }
        if (await isDuplicate(db, job)) { stats.deduped++; continue; }
        try {
          await db.collection("jobs").insertOne(job);
          stats.saved++;
          console.log(`     ➕ ${job.title} @ ${job.company_name} — ${job.job_city}, ${job.job_state}`);
        } catch (e) {
          if (e.code === 11000) { stats.deduped++; }
          else { stats.errors.push(e.message); }
        }
      }

      await new Promise((r) => setTimeout(r, 500));
    } catch (err) {
      console.error(`     ❌ ${err.message}`);
      stats.errors.push(err.message);
    }
  }

  console.log(`\n  Fantastic → Fetched: ${stats.fetched} | Saved: ${stats.saved} | Not US: ${stats.not_us} | Filtered: ${stats.filtered} | Deduped: ${stats.deduped}`);
  return stats;
}

// ─── MAIN PIPELINE ────────────────────────────────────────

export async function runPipeline1(db) {
  if (!APIFY_API_KEY) throw new Error("APIFY_API_KEY missing");
  if (!FANTASTIC_API_KEY) throw new Error("FANTASTIC_JOBS_API_KEY missing");
  const start = Date.now();
  console.log(`\n${"═".repeat(52)}`);
  console.log(`  VAYURON PIPELINE 1 — ${new Date().toISOString()}`);
  console.log(`  Sources: Apify LinkedIn + Fantastic Jobs`);
  console.log(`${"═".repeat(52)}`);

  const apifyStats     = await runApify(db);
  const fantasticStats = await runFantastic(db);

  const totalSaved    = apifyStats.saved   + fantasticStats.saved;
  const totalDeduped  = apifyStats.deduped + fantasticStats.deduped;
  const totalFiltered = apifyStats.filtered + fantasticStats.filtered;
  const totalNotUS    = apifyStats.not_us + fantasticStats.not_us;
  const duration = Math.round((Date.now() - start) / 1000);
  const hasErrors = apifyStats.errors.length + fantasticStats.errors.length > 0;

  console.log(`\n${"═".repeat(52)}`);
  console.log(`  LinkedIn saved   : ${apifyStats.saved}`);
  console.log(`  Fantastic saved  : ${fantasticStats.saved}`);
  console.log(`  ────────────────────────────────────`);
  console.log(`  Total new jobs   : ${totalSaved}`);
  console.log(`  Deduped          : ${totalDeduped}`);
  console.log(`  Not US           : ${totalNotUS}`);
  console.log(`  Filtered         : ${totalFiltered}`);
  console.log(`  Duration         : ${Math.floor(duration / 60)}m ${duration % 60}s`);
  console.log(`  Status           : ${hasErrors ? "⚠️  partial" : "✅ success"}`);
  console.log(`${"═".repeat(52)}\n`);

  const summary = {
    pipeline: "pipeline1",
    run_at: new Date(),
    apify_linkedin: apifyStats,
    fantastic_jobs: fantasticStats,
    total_new_jobs: totalSaved,
    total_deduped: totalDeduped,
    total_not_us: totalNotUS,
    total_filtered: totalFiltered,
    duration_seconds: duration,
    status: hasErrors ? "partial" : "success",
  };
  return summary;
}
