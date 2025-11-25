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
  const shouldSendJson =
    !isFormData &&
    init.body !== undefined &&
    !(typeof init.body === "string") &&
    !(init.body instanceof Blob) &&
    !(init.body instanceof ArrayBuffer) &&
    !(init.body instanceof URLSearchParams) &&
    !(typeof ReadableStream !== "undefined" && init.body instanceof ReadableStream);

  const headers = isFormData
    ? init.headers
    : init.body
      ? { ...defaultHeaders, ...(init.headers || {}) }
      : init.headers;

  const body = shouldSendJson ? JSON.stringify(init.body) : init.body;

  const response = await fetch(input, {
    credentials: "include",
    headers,
    ...init,
    body
  });

  const responseBody = await parseJson(response);
  if (!response.ok) {
    throw new ApiError(response.status, (responseBody as any)?.detail || response.statusText, responseBody);
  }
  return responseBody as T;
}
