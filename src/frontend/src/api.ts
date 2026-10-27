export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
  }
}

export async function request<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...options,
    headers: { Accept: "application/json", ...options.headers },
    signal: options.signal ?? AbortSignal.timeout(12000),
  });
  const text = await response.text();
  let data: unknown;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    throw new ApiError(
      "The server returned an unreadable response.",
      response.status,
    );
  }
  if (!response.ok) {
    const error = data as { message?: string; error?: string } | null;
    throw new ApiError(
      error?.message || error?.error || `Request failed (${response.status}).`,
      response.status,
      error?.error,
    );
  }
  return data as T;
}

export function mutation<T>(
  path: string,
  body: unknown,
  operator: string,
  idempotencyKey: string,
): Promise<T> {
  return request<T>(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
      "X-Operator": operator.trim() || "operator",
    },
    body: JSON.stringify(body ?? {}),
  });
}

export function errorMessage(error: unknown): string {
  if (error instanceof DOMException && error.name === "TimeoutError")
    return "The request timed out. Its outcome is unknown; retry with the same request ID.";
  if (error instanceof TypeError)
    return "Cannot reach the server. Its outcome is unknown; retry with the same request ID.";
  return error instanceof Error ? error.message : "The request failed.";
}
