function startOperationJob(job) {
  setTimeout(async () => {
    let current = normalizeOperationJob({
      ...job,
      status: "running",
      startedAt: new Date().toISOString(),
      progress: 5,
    });
    await upsertOperationJob(current).catch((error) => logger.warn("operation job start write failed", { detail: error?.message || String(error) }));
    try {
      let lastProgressWriteAt = 0;
      const result = await runOperationPayload(current, {
        onProgress: async (progress = {}) => {
          const nextProgress = Math.max(current.progress || 0, Math.min(99, Number(progress.progress || progress.percent || 5) || 5));
          const nowMs = Date.now();
          if (nextProgress <= current.progress && nowMs - lastProgressWriteAt < 3000) return;
          current = normalizeOperationJob({
            ...current,
            progress: nextProgress,
            result: progress.summary ? { summary: cleanText(progress.summary) } : current.result,
          });
          lastProgressWriteAt = nowMs;
          await upsertOperationJob(current).catch((error) => logger.warn("operation job progress write failed", { detail: error?.message || String(error) }));
        },
      });
      const partial = result?.partial === true;
      current = normalizeOperationJob({
        ...current,
        status: result?.ok === false && !partial ? "failed" : "completed",
        finishedAt: new Date().toISOString(),
        progress: 100,
        result,
        error: result?.ok === false ? (partial ? "operation finished partially" : "operation finished with errors") : "",
      });
    } catch (error) {
      current = normalizeOperationJob({
        ...current,
        status: "failed",
        finishedAt: new Date().toISOString(),
        progress: 100,
        error: error?.message || String(error),
      });
      logger.warn("operation job failed", { id: current.id, type: current.type, detail: current.error });
    }
    await upsertOperationJob(current).catch((error) => logger.warn("operation job finish write failed", { detail: error?.message || String(error) }));
  }, 10);
}



