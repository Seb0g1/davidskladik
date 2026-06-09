const activeOperationJobs = new Map();

function normalizeOperationJob(input = {}) {
  const id = cleanText(input.id) || crypto.randomUUID();
  const status = ["queued", "running", "completed", "failed"].includes(input.status) ? input.status : "queued";
  return {
    id,
    type: cleanText(input.type || "unknown"),
    title: cleanText(input.title || input.type || "Operation"),
    status,
    user: cleanText(input.user || "system"),
    role: cleanText(input.role || "admin"),
    createdAt: input.createdAt || new Date().toISOString(),
    startedAt: input.startedAt || null,
    finishedAt: input.finishedAt || null,
    progress: Math.max(0, Math.min(100, Number(input.progress || 0) || 0)),
    payload: input.payload && typeof input.payload === "object" ? input.payload : {},
    result: input.result && typeof input.result === "object" ? input.result : null,
    error: cleanText(input.error || ""),
  };
}

async function readOperationJobs(limit = 100) {
  try {
    const parsed = JSON.parse(await fs.readFile(operationJobsPath, "utf8"));
    const jobs = Array.isArray(parsed.jobs) ? parsed.jobs.map(normalizeOperationJob) : [];
    return jobs
      .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)))
      .slice(0, Math.max(1, Math.min(500, Number(limit || 100) || 100)));
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

async function writeOperationJobs(jobs = []) {
  await fs.mkdir(dataDir, { recursive: true });
  const normalized = (Array.isArray(jobs) ? jobs : [])
    .map(normalizeOperationJob)
    .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)))
    .slice(0, 300);
  const temporaryPath = `${operationJobsPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify({ updatedAt: new Date().toISOString(), jobs: normalized }, null, 2), "utf8");
  await fs.rename(temporaryPath, operationJobsPath);
  return normalized;
}

async function upsertOperationJob(job) {
  const normalized = normalizeOperationJob(job);
  activeOperationJobs.set(normalized.id, normalized);
  const existing = await readOperationJobs(300);
  const next = [normalized, ...existing.filter((item) => item.id !== normalized.id)];
  await writeOperationJobs(next);
  return normalized;
}

function operationJobPublic(job = {}) {
  const normalized = normalizeOperationJob(job);
  const result = normalized.result || {};
  return {
    id: normalized.id,
    type: normalized.type,
    title: normalized.title,
    status: normalized.status,
    user: normalized.user,
    createdAt: normalized.createdAt,
    startedAt: normalized.startedAt,
    finishedAt: normalized.finishedAt,
    progress: normalized.progress,
    error: normalized.error,
    summary: result.summary || null,
    result: normalized.status === "completed" || normalized.status === "failed" ? result : null,
  };
}

