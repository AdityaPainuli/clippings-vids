/**
 * Shared frontend API configuration.
 *
 * Configures the backend API base URL using NEXT_PUBLIC_API_URL, defaulting to http://localhost:8000.
 * In Next.js, NEXT_PUBLIC_* variables are inlined at build time, so NEXT_PUBLIC_API_URL must be
 * present in the build environment for production deployments.
 */
export const API_BASE_URL = (
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"
).replace(/\/+$/, "");

export default API_BASE_URL;
