import { test } from "node:test";
import assert from "node:assert/strict";

import {
  authenticatedFetch,
  storeTokens,
  getAccessToken,
  getRefreshToken,
  clearTokens,
} from "../src/lib/auth.mjs";

class MemoryStorage {
  constructor() {
    this.data = new Map();
  }

  getItem(key) {
    return this.data.get(key) ?? null;
  }

  setItem(key, value) {
    this.data.set(key, String(value));
  }

  removeItem(key) {
    this.data.delete(key);
  }
}

function response(status, data = {}) {
  return {
    status,
    ok: status >= 200 && status < 300,
    json: async () => data,
  };
}

const API_BASE_URL = "http://localhost:8000";

test("stores and clears both authentication tokens", () => {
  const storage = new MemoryStorage();

  storeTokens("access-1", "refresh-1", storage);

  assert.equal(getAccessToken(storage), "access-1");
  assert.equal(getRefreshToken(storage), "refresh-1");

  clearTokens(storage);

  assert.equal(getAccessToken(storage), null);
  assert.equal(getRefreshToken(storage), null);
});

test("refreshes after 401, rotates tokens, and retries once", async () => {
  const storage = new MemoryStorage();
  storeTokens("expired-access", "old-refresh", storage);

  const counts = new Map();
  const calls = [];

  const fetchMock = async (url, init = {}) => {
    calls.push({ url, init });

    if (url.endsWith("/auth/refresh")) {
      assert.equal(init.body.get("refresh_token"), "old-refresh");

      return response(200, {
        access_token: "new-access",
        refresh_token: "new-refresh",
      });
    }

    const count = (counts.get(url) ?? 0) + 1;
    counts.set(url, count);

    return count === 1
      ? response(401)
      : response(200, { success: true });
  };

  const result = await authenticatedFetch(
    `${API_BASE_URL}/my-clips`,
    {},
    { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
  );

  assert.equal(result.status, 200);
  assert.equal(calls.length, 3);

  assert.equal(
    new Headers(calls[0].init.headers).get("Authorization"),
    "Bearer expired-access"
  );

  assert.equal(
    new Headers(calls[2].init.headers).get("Authorization"),
    "Bearer new-access"
  );

  assert.equal(getAccessToken(storage), "new-access");
  assert.equal(getRefreshToken(storage), "new-refresh");
});

test("shares one refresh request across concurrent 401 responses", async () => {
  const storage = new MemoryStorage();
  storeTokens("expired-access", "old-refresh", storage);

  let refreshCount = 0;
  let resolveRefresh;
  let notifyRefreshStarted;

  const refreshStarted = new Promise((resolve) => {
    notifyRefreshStarted = resolve;
  });

  const delayedRefresh = new Promise((resolve) => {
    resolveRefresh = resolve;
  });

  const requestCounts = new Map();

  const fetchMock = async (url) => {
    if (url.endsWith("/auth/refresh")) {
      refreshCount++;
      notifyRefreshStarted();
      return delayedRefresh;
    }

    const count = (requestCounts.get(url) ?? 0) + 1;
    requestCounts.set(url, count);

    return count === 1
      ? response(401)
      : response(200, { success: true });
  };

  const first = authenticatedFetch(
    `${API_BASE_URL}/one`,
    {},
    { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
  );

  const second = authenticatedFetch(
    `${API_BASE_URL}/two`,
    {},
    { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
  );

  await refreshStarted;

  resolveRefresh(response(200, {
    access_token: "new-access",
    refresh_token: "new-refresh",
  }));

  const results = await Promise.all([first, second]);

  assert.equal(refreshCount, 1);
  assert.equal(results[0].status, 200);
  assert.equal(results[1].status, 200);
});

test("does not refresh or clear tokens on 403", async () => {
  const storage = new MemoryStorage();
  storeTokens("valid-access", "valid-refresh", storage);

  let refreshCount = 0;

  const fetchMock = async (url) => {
    if (url.endsWith("/auth/refresh")) refreshCount++;
    return response(403);
  };

  const result = await authenticatedFetch(
    `${API_BASE_URL}/protected`,
    {},
    { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
  );

  assert.equal(result.status, 403);
  assert.equal(refreshCount, 0);
  assert.equal(getAccessToken(storage), "valid-access");
  assert.equal(getRefreshToken(storage), "valid-refresh");
});

test("clears tokens when the refresh token is rejected", async () => {
  const storage = new MemoryStorage();
  storeTokens("expired-access", "expired-refresh", storage);

  const fetchMock = async (url) => {
    if (url.endsWith("/auth/refresh")) return response(401);
    return response(401);
  };

  const result = await authenticatedFetch(
    `${API_BASE_URL}/protected`,
    {},
    { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
  );

  assert.equal(result.status, 401);
  assert.equal(getAccessToken(storage), null);
  assert.equal(getRefreshToken(storage), null);
});

test("does not retry indefinitely after a second 401", async () => {
  const storage = new MemoryStorage();
  storeTokens("expired-access", "valid-refresh", storage);

  let requestCount = 0;

  const fetchMock = async (url) => {
    if (url.endsWith("/auth/refresh")) {
      return response(200, {
        access_token: "new-access",
        refresh_token: "new-refresh",
      });
    }

    requestCount++;
    return response(401);
  };

  const result = await authenticatedFetch(
    `${API_BASE_URL}/protected`,
    {},
    { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
  );

  assert.equal(result.status, 401);
  assert.equal(requestCount, 2);
  assert.equal(getAccessToken(storage), null);
  assert.equal(getRefreshToken(storage), null);
});

test("preserves tokens when refresh fails due to a network error", async () => {
  const storage = new MemoryStorage();
  storeTokens("expired-access", "valid-refresh", storage);

  const fetchMock = async (url) => {
    if (url.endsWith("/auth/refresh")) {
      throw new Error("Network unavailable");
    }
    return response(401);
  };

  await assert.rejects(
    authenticatedFetch(
      `${API_BASE_URL}/protected`,
      {},
      { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
    ),
    /Network unavailable/
  );

  assert.equal(getAccessToken(storage), "expired-access");
  assert.equal(getRefreshToken(storage), "valid-refresh");
});
test("preserves tokens when refresh endpoint returns HTTP 500", async () => {
  const storage = new MemoryStorage();
  storeTokens("expired-access", "valid-refresh", storage);

  const fetchMock = async (url) => {
    if (url.endsWith("/auth/refresh")) {
      return response(500, { detail: "Internal server error" });
    }

    return response(401);
  };

  await assert.rejects(
    authenticatedFetch(
      `${API_BASE_URL}/protected`,
      {},
      { apiBaseUrl: API_BASE_URL, fetchImpl: fetchMock, storage }
    ),
    /Token refresh failed with status 500/
  );

  assert.equal(getAccessToken(storage), "expired-access");
  assert.equal(getRefreshToken(storage), "valid-refresh");
});