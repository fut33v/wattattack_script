export class ApiError extends Error {
  status: number;
  body: unknown;

  constructor(status: number, message: string, body: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

const defaultHeaders = {
  "Content-Type": "application/json"
};

async function parseJson(response: Response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (err) {
    return text;
  }
}

export async function apiFetch<T>(input: RequestInfo, init: RequestInit = {}): Promise<T> {
  const isFormData = typeof FormData !== "undefined" && init.body instanceof FormData;
  const headers = isFormData
    ? init.headers
    : init.body
      ? { ...defaultHeaders, ...(init.headers || {}) }
      : init.headers;

  const response = await fetch(input, {
    credentials: "include",
    headers,
    ...init
  });

  const body = await parseJson(response);
  if (!response.ok) {
    throw new ApiError(response.status, (body as any)?.detail || response.statusText, body);
  }
  return body as T;
}
