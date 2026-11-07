const ACCESS_TOKEN_KEY = "clipwave_access_token";
const REFRESH_TOKEN_KEY = "clipwave_refresh_token";

const refreshPromises = new WeakMap();
const sessionGenerations = new WeakMap();

export class StaleAuthRequestError extends Error {
  constructor() {
    super("Authentication session changed while the request was pending.");
    this.name = "StaleAuthRequestError";
  }
}

function getBrowserStorage() {
  if (typeof window === "undefined") {
    throw new Error("Browser storage is unavailable.");
  }

  return window.localStorage;
}

export function getSessionGeneration(
  storage = getBrowserStorage()
) {
  return sessionGenerations.get(storage) ?? 0;
}

export function isSessionGenerationCurrent(
  generation,
  storage = getBrowserStorage()
) {
  return getSessionGeneration(storage) === generation;
}

function advanceSessionGeneration(storage) {
  const next = getSessionGeneration(storage) + 1;
  sessionGenerations.set(storage, next);
  return next;
}

function assertSessionCurrent(storage, generation) {
  if (getSessionGeneration(storage) !== generation) {
    throw new StaleAuthRequestError();
  }
}

function writeTokens(accessToken, refreshToken, storage) {
  storage.setItem(ACCESS_TOKEN_KEY, accessToken);
  storage.setItem(REFRESH_TOKEN_KEY, refreshToken);
}

function removeTokens(storage) {
  storage.removeItem(ACCESS_TOKEN_KEY);
  storage.removeItem(REFRESH_TOKEN_KEY);
}

export function storeTokens(
  accessToken,
  refreshToken,
  storage = getBrowserStorage()
) {
  if (
    typeof accessToken !== "string" ||
    !accessToken ||
    typeof refreshToken !== "string" ||
    !refreshToken
  ) {
    throw new Error("Both authentication tokens are required.");
  }

  advanceSessionGeneration(storage);
  writeTokens(accessToken, refreshToken, storage);
}

export function getAccessToken(storage = getBrowserStorage()) {
  return storage.getItem(ACCESS_TOKEN_KEY);
}

export function getRefreshToken(storage = getBrowserStorage()) {
  return storage.getItem(REFRESH_TOKEN_KEY);
}

export function clearTokens(storage = getBrowserStorage()) {
  advanceSessionGeneration(storage);
  removeTokens(storage);
}

async function performRefresh(
  apiBaseUrl,
  fetchImpl,
  storage,
  generation
) {
  assertSessionCurrent(storage, generation);

  const refreshToken = getRefreshToken(storage);

  if (!refreshToken) {
    removeTokens(storage);
    return false;
  }

  const formData = new FormData();
  formData.append("refresh_token", refreshToken);

  const response = await fetchImpl(`${apiBaseUrl}/auth/refresh`, {
    method: "POST",
    body: formData,
  });

  assertSessionCurrent(storage, generation);

  if (getRefreshToken(storage) !== refreshToken) {
    throw new StaleAuthRequestError();
  }

  if (!response.ok) {
    if (response.status === 401) {
      removeTokens(storage);
      return false;
    }

    throw new Error(
      `Token refresh failed with status ${response.status}.`
    );
  }

  const data = await response.json();

  assertSessionCurrent(storage, generation);

  if (getRefreshToken(storage) !== refreshToken) {
    throw new StaleAuthRequestError();
  }

  if (
    typeof data.access_token !== "string" ||
    !data.access_token ||
    typeof data.refresh_token !== "string" ||
    !data.refresh_token
  ) {
    throw new Error("Token refresh returned an invalid response.");
  }

  // Token rotation keeps the same login session generation.
  writeTokens(data.access_token, data.refresh_token, storage);

  return true;
}

function refreshOnce(apiBaseUrl, fetchImpl, storage, generation) {
  let refreshes = refreshPromises.get(storage);

  if (!refreshes) {
    refreshes = new Map();
    refreshPromises.set(storage, refreshes);
  }

  const existing = refreshes.get(generation);

  if (existing) {
    return existing;
  }

  let pending;

  pending = performRefresh(
    apiBaseUrl,
    fetchImpl,
    storage,
    generation
  ).finally(() => {
    if (refreshes.get(generation) === pending) {
      refreshes.delete(generation);
    }

    if (refreshes.size === 0) {
      refreshPromises.delete(storage);
    }
  });

  refreshes.set(generation, pending);

  return pending;
}

async function sendWithToken(input, init, token, fetchImpl) {
  const headers = new Headers(init.headers || {});

  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  } else {
    headers.delete("Authorization");
  }

  return fetchImpl(input, {
    ...init,
    headers,
  });
}

export async function authenticatedFetch(
  input,
  init = {},
  options = {}
) {
  const {
    apiBaseUrl,
    fetchImpl = globalThis.fetch,
    storage = getBrowserStorage(),
  } = options;

  if (!apiBaseUrl) {
    throw new Error("API base URL is required.");
  }

  if (typeof fetchImpl !== "function") {
    throw new Error("Fetch implementation is unavailable.");
  }

  const generation = getSessionGeneration(storage);
  const originalToken = getAccessToken(storage);

  const firstResponse = await sendWithToken(
    input,
    init,
    originalToken,
    fetchImpl
  );

  assertSessionCurrent(storage, generation);

  if (firstResponse.status !== 401) {
    return firstResponse;
  }

  const currentToken = getAccessToken(storage);

  let canRetry = false;

  if (currentToken && currentToken !== originalToken) {
    canRetry = true;
  } else {
    try {
      canRetry = await refreshOnce(
        apiBaseUrl,
        fetchImpl,
        storage,
        generation
      );
    } catch (error) {
      assertSessionCurrent(storage, generation);
      throw error;
    }
  }

  assertSessionCurrent(storage, generation);

  if (!canRetry) {
    return firstResponse;
  }

  const newToken = getAccessToken(storage);

  if (!newToken) {
    return firstResponse;
  }

  assertSessionCurrent(storage, generation);

  const retryResponse = await sendWithToken(
    input,
    init,
    newToken,
    fetchImpl
  );

  assertSessionCurrent(storage, generation);

  if (retryResponse.status === 401) {
    removeTokens(storage);
  }

  return retryResponse;
}
