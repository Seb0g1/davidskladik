function defaultLinkedReconcilerState() {
  return {
    lastProductId: null,
    cycleStartedAt: null,
    cyclesCompleted: 0,
    processedThisCycle: 0,
    processedTotal: 0,
    lastBatchAt: null,
    lastError: null,
    updatedAt: null,
  };
}

async function readLinkedReconcilerState() {
  try {
    const parsed = JSON.parse(await fs.readFile(linkedReconcilerStatePath, "utf8"));
    return { ...defaultLinkedReconcilerState(), ...(parsed && typeof parsed === "object" ? parsed : {}) };
  } catch (error) {
    if (error.code === "ENOENT") return defaultLinkedReconcilerState();
    throw error;
  }
}

async function writeLinkedReconcilerState(state = {}) {
  await fs.mkdir(dataDir, { recursive: true });
  const current = await readLinkedReconcilerState().catch(() => defaultLinkedReconcilerState());
  const payload = {
    ...current,
    ...state,
    updatedAt: new Date().toISOString(),
  };
  const temporaryPath = `${linkedReconcilerStatePath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await fs.rename(temporaryPath, linkedReconcilerStatePath);
      break;
    } catch (error) {
      if (attempt === 4 || !["EPERM", "EBUSY", "EACCES"].includes(error.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 80 * (attempt + 1)));
    }
  }
  return payload;
}

async function getLinkedReconcilerStatus() {
  const state = await readLinkedReconcilerState().catch(() => defaultLinkedReconcilerState());
  return {
    ...state,
    enabled: linkedReconcilerEnabled,
    running: linkedReconcilerRunning,
    nextRunAt: linkedReconcilerNextRunAt,
    serverRole,
    batchSize: linkedReconcilerBatchSize,
    intervalMinutes: linkedReconcilerIntervalMinutes,
    maxProductsPerTick: linkedReconcilerMaxProductsPerTick,
    sendTargetStock: linkedReconcilerSendTargetStock,
  };
}
