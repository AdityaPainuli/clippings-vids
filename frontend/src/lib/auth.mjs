
const ACCESS_TOKEN_KEY = "clipwave_access_token";
const REFRESH_TOKEN_KEY = "clipwave_refresh_token";

// One refresh operation per browser storage instance.
const refreshPromises = new WeakMap();

function getBrowserStorage() {
  if (typeof window === "undefined") {
    throw new Error("Browser storage is unavailable.");
  }

  return window.localStorage;
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

  storage.setItem(ACCESS_TOKEN_KEY, accessToken);
  storage.setItem(REFRESH_TOKEN_KEY, refreshToken);
}

export function getAccessToken(storage = getBrowserStorage()) {
  return storage.getItem(ACCESS_TOKEN_KEY);
}

export function getRefreshToken(storage = getBrowserStorage()) {
  return storage.getItem(REFRESH_TOKEN_KEY);
}

export function clearTokens(storage = getBrowserStorage()) {
  storage.removeItem(ACCESS_TOKEN_KEY);
  storage.removeItem(REFRESH_TOKEN_KEY);
}

async function performRefresh(apiBaseUrl, fetchImpl, storage) {
  const refreshToken = getRefreshToken(storage);

  if (!refreshToken) {
    clearTokens(storage);
    return false;
  }

  const formData = new FormData();
  formData.append("refresh_token", refreshToken);

  const response = await fetchImpl(`${apiBaseUrl}/auth/refresh`, {
    method: "POST",
    body: formData,
  });

    if (!response.ok) {
    // Invalid or revoked refresh token: the session cannot be recovered.
    if ([400, 401, 403].includes(response.status)) {
      clearTokens(storage);
      return false;
    }

    // Temporary server errors must not force the user to log out.
    throw new Error(
      `Token refresh failed with status ${response.status}.`
    );
  }

  const data = await response.json();

  if (
    typeof data.access_token !== "string" ||
    !data.access_token ||
    typeof data.refresh_token !== "string" ||
    !data.refresh_token
  ) {
    clearTokens(storage);
    return false;
  }

  // Supabase rotates both tokens. Always persist the complete new pair.
  storeTokens(data.access_token, data.refresh_token, storage);

  return true;
}

function refreshOnce(apiBaseUrl, fetchImpl, storage) {
  const existingRefresh = refreshPromises.get(storage);

  if (existingRefresh) {
    return existingRefresh;
  }

  const pendingRefresh = performRefresh(
    apiBaseUrl,
    fetchImpl,
    storage
  ).finally(() => {
    refreshPromises.delete(storage);
  });

  refreshPromises.set(storage, pendingRefresh);

  return pendingRefresh;
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

  const originalToken = getAccessToken(storage);

  // Attempt the original request.
  const firstResponse = await sendWithToken(
    input,
    init,
    originalToken,
    fetchImpl
  );

  // Only 401 indicates that we should try refreshing.
  if (firstResponse.status !== 401) {
    return firstResponse;
  }

  // Another request may already have refreshed the session.
  // Reuse its new access token instead of refreshing again.
  const currentToken = getAccessToken(storage);

  let canRetry = false;

  if (currentToken && currentToken !== originalToken) {
    canRetry = true;
  } else {
    canRetry = await refreshOnce(
      apiBaseUrl,
      fetchImpl,
      storage
    );
  }

  if (!canRetry) {
    return firstResponse;
  }

  const newToken = getAccessToken(storage);

  if (!newToken) {
    return firstResponse;
  }

  // Retry the original request exactly once.
  const retryResponse = await sendWithToken(
    input,
    init,
    newToken,
    fetchImpl
  );

  // A second 401 means the renewed session is also unusable.
  if (retryResponse.status === 401) {
    clearTokens(storage);
  }

  return retryResponse;
}
