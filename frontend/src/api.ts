import { z } from "zod";

export class ApiError extends Error {
  status: number;
  code?: string;
  detail?: unknown;

  constructor(message: string, status: number, code?: string, detail?: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

export async function fetchJson<T>(url: string, schema: z.ZodType<T>, init: RequestInit = {}): Promise<T> {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...init,
    headers: {
      ...(init.body ? { "Content-Type": "application/json" } : {}),
      ...(init.headers || {}),
    },
  });

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json().catch(() => ({}))
    : await response.text().catch(() => "");

  if (response.status === 401) {
    if (import.meta.env.DEV) {
      console.warn("401: войди через /login.html (dev-режим не редиректит)");
    } else {
      window.location.href = "/login.html";
    }
    throw new ApiError("Требуется вход", response.status, "unauthorized", payload);
  }

  if (!response.ok) {
    const message = typeof payload === "object" && payload
      ? String((payload as { error?: string; detail?: string }).error || (payload as { detail?: string }).detail || `HTTP ${response.status}`)
      : String(payload || `HTTP ${response.status}`);
    throw new ApiError(message, response.status, typeof payload === "object" && payload ? (payload as { code?: string }).code : undefined, payload);
  }

  try {
    return schema.parse(payload);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const issues = error.issues.slice(0, 25).map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
        code: issue.code,
      }));
      console.error("[api] schema validation failed", { url, issues });
      throw new ApiError(
        issues.map((issue) => `${issue.path}: ${issue.message}`).join("; ") || "Ошибка валидации ответа",
        response.status,
        "schema_validation",
        issues,
      );
    }
    throw error;
  }
}

export function mutationBody(input: unknown): RequestInit {
  return {
    method: "POST",
    body: JSON.stringify(input),
  };
}

export function patchBody(input: unknown): RequestInit {
  return {
    method: "PATCH",
    body: JSON.stringify(input),
  };
}
