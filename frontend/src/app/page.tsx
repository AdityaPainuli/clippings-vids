"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import "./globals.css";

interface Clip {
  url?: string;
  path?: string;
  src?: string;
  description?: string;
  hook?: string;
  virality_score?: number;
  clip_type?: string;
  start_time?: number;
  end_time?: number;
}

const CLIP_STYLES = [
  { id: "auto", label: "Auto", icon: "🎯", desc: "Let AI decide" },
  { id: "funny", label: "Funny", icon: "😂", desc: "Comedy gold" },
  { id: "educational", label: "Educational", icon: "🧠", desc: "Key insights" },
  { id: "emotional", label: "Emotional", icon: "💫", desc: "Powerful moments" },
  { id: "controversial", label: "Controversial", icon: "🔥", desc: "Hot takes" },
  { id: "highlights", label: "Highlights", icon: "⚡", desc: "Peak action" },
];

const CAPTION_STYLES = [
  { id: "default", label: "Classic", desc: "Static highlight" },
  { id: "bold_impact", label: "Bold Impact", desc: "Pop-in animation" },
  { id: "subtle", label: "Subtle", desc: "Smooth fade-in" },
  { id: "karaoke", label: "Karaoke", desc: "Progressive fill" },
];

export default function Home() {
  const [url, setUrl] = useState("");
  const [instructions, setInstructions] = useState("");
  const [clipStyle, setClipStyle] = useState("auto");
  const [captionStyle, setCaptionStyle] = useState("bold_impact");
  const [clipCount, setClipCount] = useState<number>(4);
  const [minLength, setMinLength] = useState<number>(20);
  const [maxLength, setMaxLength] = useState<number>(60);
  const [isProcessing, setIsProcessing] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<string>("");
  const [statusDetail, setStatusDetail] = useState<string>("");
  const [clips, setClips] = useState<Clip[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [warning, setWarning] = useState<string | null>(null);

  // Trimmer modal state
  const [trimmerClip, setTrimmerClip] = useState<Clip | null>(null);
  const [trimStart, setTrimStart] = useState(0);
  const [trimEnd, setTrimEnd] = useState(0);
  const [trimDuration, setTrimDuration] = useState(0);   // actual video duration
  const trimVideoRef = useRef<HTMLVideoElement>(null);

  // --- Auth State ---
  const [token, setToken] = useState<string | null>(null);
  const [userEmail, setUserEmail] = useState<string | null>(null);

  const [showAuthModal, setShowAuthModal] = useState(false);
  const [authMode, setAuthMode] = useState<"login" | "signup">("login");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authError, setAuthError] = useState<string | null>(null);
  const [isAuthLoading, setIsAuthLoading] = useState(false);

  // SSE + polling refs for cleanup
  const eventSourceRef = useRef<EventSource | null>(null);
  const pollIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = () => {
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
      pollIntervalRef.current = null;
    }
  };

  useEffect(() => {
    const storedToken = localStorage.getItem("clipwave_access_token");
    const storedEmail = localStorage.getItem("clipwave_user_email");
    if (storedToken && storedEmail) {
      setToken(storedToken);
      setUserEmail(storedEmail);
    }
  }, []);

  // Cleanup SSE + polling on unmount
  useEffect(() => {
    return () => {
      eventSourceRef.current?.close();
      stopPolling();
    };
  }, []);

  const handleLogout = () => {
    localStorage.removeItem("clipwave_access_token");
    localStorage.removeItem("clipwave_user_email");
    setToken(null);
    setUserEmail(null);
    setClips([]);
    setJobId(null);
    eventSourceRef.current?.close();
    stopPolling();
  };

  const handleAuthSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsAuthLoading(true);
    setAuthError(null);

    const endpoint = authMode === "login" ? "/auth/login" : "/auth/signup";

    try {
      const formData = new FormData();
      formData.append("email", authEmail);
      formData.append("password", authPassword);

      const response = await fetch(`http://localhost:8000${endpoint}`, {
        method: "POST",
        body: formData,
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || data.message || "Authentication failed");
      }

      if (authMode === "login") {
        localStorage.setItem("clipwave_access_token", data.access_token);
        localStorage.setItem("clipwave_user_email", data.email);
        setToken(data.access_token);
        setUserEmail(data.email);
        setShowAuthModal(false);
        setAuthEmail("");
        setAuthPassword("");
      } else {
        setAuthError("Signup successful! You can now log in.");
        setAuthMode("login");
      }
    } catch (err: any) {
      setAuthError(err.message);
    } finally {
      setIsAuthLoading(false);
    }
  };

  const openAuth = (mode: "login" | "signup") => {
    setAuthMode(mode);
    setAuthError(null);
    setShowAuthModal(true);
  };

  // Connect to SSE stream for a job.
  // The bearer JWT never goes in the URL — we exchange it for a one-time
  // short-lived stream token first (query strings leak via logs/history).
  const connectSSE = useCallback(async (jobId: string, authToken: string) => {
    eventSourceRef.current?.close();
    stopPolling();

    let streamToken: string;
    try {
      const tokenRes = await fetch("http://localhost:8000/stream-token", {
        method: "POST",
        headers: { "Authorization": `Bearer ${authToken}` },
      });
      if (!tokenRes.ok) throw new Error("stream token request failed");
      streamToken = (await tokenRes.json()).stream_token;
    } catch {
      // Can't get a stream token — fall back to polling
      startPolling(jobId, authToken);
      return;
    }

    const es = new EventSource(
      `http://localhost:8000/stream/${jobId}?token=${encodeURIComponent(streamToken)}`
    );
    eventSourceRef.current = es;

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setStatus(data.status || "");
        if (data.detail) setStatusDetail(data.detail);

        if (data.status === "completed") {
          setClips(data.results || []);
          if (data.warnings) setWarning(data.warnings);
          setIsProcessing(false);
          setJobId(null);
          es.close();
        } else if (data.status === "failed") {
          setError(data.error || "Processing failed");
          setIsProcessing(false);
          setJobId(null);
          es.close();
        }
      } catch {
        // ignore parse errors on heartbeats
      }
    };

    es.onerror = () => {
      // SSE failed — fall back to polling
      es.close();
      console.warn("SSE connection lost, falling back to polling");
      startPolling(jobId, authToken);
    };
  }, []);

  // Fallback polling (in case SSE fails)
  const startPolling = useCallback((jobId: string, authToken: string) => {
    stopPolling();
    const interval = setInterval(async () => {
      try {
        const response = await fetch(`http://localhost:8000/status/${jobId}`, {
          headers: { "Authorization": `Bearer ${authToken}` }
        });
        const data = await response.json();

        if (!response.ok) {
          if (response.status === 401 || response.status === 403) {
            handleLogout();
            throw new Error("Session expired. Please log in again.");
          }
          throw new Error(data.detail || "Failed to fetch status");
        }

        setStatus(data.status);

        if (data.status === "completed") {
          setClips(data.results);
          if (data.warnings) setWarning(data.warnings);
          setIsProcessing(false);
          setJobId(null);
          clearInterval(interval);
        } else if (data.status === "failed") {
          setError(data.error || "Processing failed");
          setIsProcessing(false);
          setJobId(null);
          clearInterval(interval);
        }
      } catch (err: any) {
        console.error("Polling error:", err);
        setError(err.message);
        if (err.message.includes("Session expired")) {
          setIsProcessing(false);
          setJobId(null);
          clearInterval(interval);
        }
      }
    }, 2000);

    pollIntervalRef.current = interval;
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!url) return;

    if (!token) {
      setError("Please log in to generate clips.");
      openAuth("login");
      return;
    }

    setIsProcessing(true);
    setError(null);
    setWarning(null);
    setClips([]);
    setStatus("Initiating...");
    setStatusDetail("");

    try {
      const formData = new FormData();
      formData.append("url", url);
      if (instructions) formData.append("instructions", instructions);
      formData.append("clip_style", clipStyle);
      formData.append("caption_style", captionStyle);
      formData.append("clip_count", String(clipCount));
      formData.append("min_clip_length", String(minLength));
      formData.append("max_clip_length", String(maxLength));

      const response = await fetch("http://localhost:8000/process-url", {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${token}`
        },
        body: formData,
      });

      const data = await response.json();

      if (!response.ok) {
        if (response.status === 401 || response.status === 403) {
          handleLogout();
          throw new Error("Session expired. Please log in again.");
        }
        throw new Error(data.detail || "Failed to start processing");
      }

      setJobId(data.job_id);

      // If cached, results are already in the response
      if (data.status === "completed") {
        // Fetch full status to get results
        const statusRes = await fetch(`http://localhost:8000/status/${data.job_id}`, {
          headers: { "Authorization": `Bearer ${token}` }
        });
        const statusData = await statusRes.json();
        if (!statusRes.ok) {
          if (statusRes.status === 401 || statusRes.status === 403) {
            handleLogout();
            throw new Error("Session expired. Please log in again.");
          }
          throw new Error(statusData.detail || "Failed to fetch cached results");
        }
        setClips(statusData.results || []);
        setIsProcessing(false);
        setJobId(null);
      } else {
        // Connect to SSE stream
        connectSSE(data.job_id, token);
      }
    } catch (err: any) {
      setError(err.message);
      setIsProcessing(false);
    }
  };

  // ── Trimmer functions ──────────────────────────────────────────────────────
  const openTrimmer = (clip: Clip) => {
    setTrimmerClip(clip);
    setTrimStart(0);
    // Fallback duration until metadata loads
    const fallback = clip.end_time && clip.start_time ? clip.end_time - clip.start_time : 60;
    setTrimEnd(fallback);
    setTrimDuration(fallback);
    const videoEl = document.createElement("video");
    videoEl.src = getClipUrl(clip);
    videoEl.onloadedmetadata = () => {
      setTrimEnd(videoEl.duration);
      setTrimDuration(videoEl.duration);
    };
  };

  const closeTrimmer = () => {
    setTrimmerClip(null);
  };

  const handleTrimPreview = () => {
    const video = trimVideoRef.current;
    if (!video) return;
    video.currentTime = trimStart;
    video.play();
  };

  // Enforce trim end when video plays
  useEffect(() => {
    const video = trimVideoRef.current;
    if (!video || !trimmerClip) return;
    const handleTimeUpdate = () => {
      if (video.currentTime >= trimEnd) {
        video.pause();
        video.currentTime = trimEnd;
      }
    };
    video.addEventListener("timeupdate", handleTimeUpdate);
    return () => video.removeEventListener("timeupdate", handleTimeUpdate);
  }, [trimEnd, trimmerClip]);

  const handleTrimDownload = () => {
    if (!trimmerClip) return;
    // Downloads the full clip — browsers can't trim on download, and there's
    // no server-side trim endpoint yet. The sliders only affect preview.
    const a = document.createElement("a");
    a.href = getClipUrl(trimmerClip);
    a.download = `clip.mp4`;
    a.target = "_blank";
    a.click();
  };

  const getClipUrl = (clip: Clip) =>
    clip.url || clip.src || (clip.path ? (clip.path.startsWith("http") ? clip.path : `http://localhost:8000${clip.path}`) : "");

  return (
    <main>
      <div className="glow-bg"></div>

      {/* Auth Modal */}
      {showAuthModal && (
        <div className="modal-overlay" style={{
          position: 'fixed', top: 0, left: 0, width: '100%', height: '100%',
          backgroundColor: 'rgba(0,0,0,0.7)', zIndex: 1000,
          display: 'flex', justifyContent: 'center', alignItems: 'center'
        }}>
          <div className="modal-content feature-card" style={{
            padding: '2rem', width: '100%', maxWidth: '400px',
            position: 'relative'
          }}>
            <button
              onClick={() => setShowAuthModal(false)}
              style={{
                position: 'absolute', top: '10px', right: '15px',
                background: 'none', border: 'none', color: '#fff',
                fontSize: '1.5rem', cursor: 'pointer'
              }}
            >
              &times;
            </button>
            <h2 style={{ marginBottom: '1.5rem', textAlign: 'center' }}>
              {authMode === "login" ? "Welcome Back" : "Create Account"}
            </h2>

            {authError && (
              <div style={{
                marginBottom: '1rem',
                color: authError.includes("successful") ? '#4ade80' : '#ff4b4b',
                textAlign: 'center', fontSize: '0.9rem'
              }}>
                {authError}
              </div>
            )}

            <form onSubmit={handleAuthSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
              <input
                type="email"
                placeholder="Email"
                required
                value={authEmail}
                onChange={(e) => setAuthEmail(e.target.value)}
                style={{
                  padding: '0.8rem', borderRadius: '8px',
                  border: '1px solid var(--glass-border)',
                  background: 'rgba(255,255,255,0.05)', color: '#fff'
                }}
              />
              <input
                type="password"
                placeholder="Password"
                required
                value={authPassword}
                onChange={(e) => setAuthPassword(e.target.value)}
                style={{
                  padding: '0.8rem', borderRadius: '8px',
                  border: '1px solid var(--glass-border)',
                  background: 'rgba(255,255,255,0.05)', color: '#fff'
                }}
              />
              <button type="submit" className="btn btn-primary" disabled={isAuthLoading}>
                {isAuthLoading ? "Please wait..." : (authMode === "login" ? "Login" : "Sign Up")}
              </button>
            </form>
            <div style={{ marginTop: '1rem', textAlign: 'center', fontSize: '0.9rem', color: 'var(--text-secondary)' }}>
              {authMode === "login" ? "Don't have an account? " : "Already have an account? "}
              <button
                onClick={() => {
                  setAuthMode(authMode === "login" ? "signup" : "login");
                  setAuthError(null);
                }}
                style={{ background: 'none', border: 'none', color: 'var(--primary)', cursor: 'pointer', textDecoration: 'underline' }}
              >
                {authMode === "login" ? "Sign Up" : "Login"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Trimmer Modal */}
      {trimmerClip && (
        <div className="modal-overlay" style={{
          position: 'fixed', top: 0, left: 0, width: '100%', height: '100%',
          backgroundColor: 'rgba(0,0,0,0.85)', zIndex: 1000,
          display: 'flex', justifyContent: 'center', alignItems: 'center',
          padding: '2rem'
        }}>
          <div className="trimmer-modal">
            <button className="trimmer-close" onClick={closeTrimmer}>&times;</button>
            <h2 style={{ marginBottom: '1rem' }}>Clip Editor</h2>

            <div className="trimmer-video-wrap">
              <video
                ref={trimVideoRef}
                src={getClipUrl(trimmerClip)}
                controls
                style={{ width: '100%', maxHeight: '60vh', borderRadius: '12px' }}
              />
            </div>

            <div className="trimmer-controls">
              <div className="trimmer-range-row">
                <label>
                  <span>Start: {trimStart.toFixed(1)}s</span>
                  <input
                    type="range"
                    min={0}
                    max={Math.max(0.1, trimEnd - 1)}
                    step={0.1}
                    value={trimStart}
                    onChange={(e) => {
                      const v = parseFloat(e.target.value);
                      setTrimStart(v);
                      if (trimVideoRef.current) trimVideoRef.current.currentTime = v;
                    }}
                  />
                </label>
                <label>
                  <span>End: {trimEnd.toFixed(1)}s</span>
                  <input
                    type="range"
                    min={Math.min(trimStart + 1, trimDuration)}
                    max={trimDuration || trimEnd}
                    step={0.1}
                    value={trimEnd}
                    onChange={(e) => setTrimEnd(parseFloat(e.target.value))}
                  />
                </label>
              </div>

              <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1rem' }}>
                <button className="btn btn-secondary" onClick={handleTrimPreview}>
                  Preview Trim
                </button>
                <button className="btn btn-primary" onClick={handleTrimDownload}>
                  Download
                </button>
              </div>
            </div>

            {trimmerClip.description && (
              <p style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', marginTop: '1rem' }}>
                {trimmerClip.description}
              </p>
            )}
          </div>
        </div>
      )}

      <nav className="navbar">
        <div className="logo">Clipwave AI</div>
        <div className="action-buttons">
          {token ? (
            <>
              <a href="/my-clips" className="btn btn-secondary" style={{ marginRight: '1rem', textDecoration: 'none' }}>
                My Library
              </a>
              <span style={{ color: 'var(--text-secondary)', marginRight: '1rem', display: 'flex', alignItems: 'center' }}>
                {userEmail}
              </span>
              <button className="btn btn-secondary" onClick={handleLogout}>Logout</button>
            </>
          ) : (
            <>
              <button className="btn btn-secondary" onClick={() => openAuth("login")}>Login</button>
              <button className="btn btn-primary" onClick={() => openAuth("signup")}>Sign Up</button>
            </>
          )}
        </div>
      </nav>

      <div className="hero">
        <h1>Transform Hours into<br />Instant Highlights.</h1>
        <p>
          Automatically generate engaging short clips from your long-form videos with our powerful Clipping Engine.
        </p>

        <section style={{ width: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          <form onSubmit={handleSubmit} className="input-container">
            <input
              type="text"
              placeholder="Paste YouTube, Twitch, or video URL..."
              className="url-input"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              disabled={isProcessing}
            />
            <div className="action-buttons">
              <button type="submit" className="btn btn-primary" disabled={isProcessing}>
                {isProcessing ? `Status: ${status}` : "Generate Clips"}
              </button>
            </div>
          </form>

          {isProcessing && (
            <div className="status-banner">
              <div className="status-dot"></div>
              <div>
                <strong>{status}</strong>
                {statusDetail && <span style={{ color: 'var(--text-secondary)', marginLeft: '0.5rem' }}>— {statusDetail}</span>}
              </div>
            </div>
          )}

          {error && (
            <div style={{ marginTop: '1rem', color: '#ff4b4b' }}>
              Error: {error}
            </div>
          )}

          <div style={{ marginTop: '2rem', width: '100%', maxWidth: '700px' }}>
            <textarea
              placeholder="Add special instructions (optional): e.g. 'Find all funny moments', 'Clip the best action parts'..."
              style={{
                width: '100%',
                background: 'var(--glass-bg)',
                border: '1px solid var(--glass-border)',
                borderRadius: '16px',
                padding: '1rem',
                color: '#fff',
                fontSize: '1rem',
                minHeight: '80px',
                backdropFilter: 'var(--glass-blur)',
                outline: 'none',
                resize: 'vertical'
              }}
              value={instructions}
              onChange={(e) => setInstructions(e.target.value)}
              disabled={isProcessing}
            />
          </div>

          {/* Clip Style Selector */}
          <div style={{ marginTop: '1.5rem', width: '100%', maxWidth: '700px' }}>
            <label style={{ fontSize: '0.9rem', color: 'var(--text-secondary)', marginBottom: '0.5rem', display: 'block' }}>
              Clip Style
            </label>
            <div className="style-selector">
              {CLIP_STYLES.map((s) => (
                <button
                  key={s.id}
                  className={`style-card ${clipStyle === s.id ? 'active' : ''}`}
                  onClick={() => setClipStyle(s.id)}
                  disabled={isProcessing}
                  type="button"
                >
                  <span className="style-icon">{s.icon}</span>
                  <span className="style-label">{s.label}</span>
                  <span className="style-desc">{s.desc}</span>
                </button>
              ))}
            </div>
          </div>

          {/* Caption Style Selector */}
          <div style={{ marginTop: '1rem', width: '100%', maxWidth: '700px' }}>
            <label style={{ fontSize: '0.9rem', color: 'var(--text-secondary)', marginBottom: '0.5rem', display: 'block' }}>
              Caption Style
            </label>
            <div className="style-selector">
              {CAPTION_STYLES.map((s) => (
                <button
                  key={s.id}
                  className={`style-card style-card-sm ${captionStyle === s.id ? 'active' : ''}`}
                  onClick={() => setCaptionStyle(s.id)}
                  disabled={isProcessing}
                  type="button"
                >
                  <span className="style-label">{s.label}</span>
                  <span className="style-desc">{s.desc}</span>
                </button>
              ))}
            </div>
          </div>

          {/* Clip Count & Duration Controls */}
          <div style={{ marginTop: '1rem', width: '100%', maxWidth: '700px' }}>
            <label style={{ fontSize: '0.9rem', color: 'var(--text-secondary)', marginBottom: '0.5rem', display: 'block' }}>
              Clip Settings
            </label>
            <div className="clip-settings-row">
              <div className="clip-setting">
                <span className="clip-setting-label">Clips</span>
                <div className="clip-setting-control">
                  <button type="button" onClick={() => setClipCount(Math.max(1, clipCount - 1))} disabled={isProcessing}>-</button>
                  <span className="clip-setting-value">{clipCount}</span>
                  <button type="button" onClick={() => setClipCount(Math.min(10, clipCount + 1))} disabled={isProcessing}>+</button>
                </div>
              </div>
              <div className="clip-setting">
                <span className="clip-setting-label">Min {minLength}s</span>
                <input
                  type="range" min={5} max={maxLength - 5} step={5}
                  value={minLength}
                  onChange={(e) => setMinLength(parseInt(e.target.value))}
                  disabled={isProcessing}
                />
              </div>
              <div className="clip-setting">
                <span className="clip-setting-label">Max {maxLength}s</span>
                <input
                  type="range" min={minLength + 5} max={180} step={5}
                  value={maxLength}
                  onChange={(e) => setMaxLength(parseInt(e.target.value))}
                  disabled={isProcessing}
                />
              </div>
            </div>
          </div>

          {warning && clips.length > 0 && (
            <div className="warning-banner">
              {warning} — showing {clips.length} successful clip{clips.length !== 1 ? 's' : ''}.
            </div>
          )}
        </section>

        {clips && clips.length > 0 && (
          <section style={{ marginTop: '4rem', width: '100%' }}>
            <h2 style={{ marginBottom: '2rem' }}>Your Viral Clips</h2>
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
              gap: '2rem'
            }}>
              {clips.map((clip, index) => (
                <div key={index} className="feature-card" style={{ padding: '1rem' }}>
                  <div style={{
                    aspectRatio: '9/16',
                    background: '#000',
                    borderRadius: '12px',
                    marginBottom: '1rem',
                    overflow: 'hidden',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center'
                  }}>
                    <video
                      src={getClipUrl(clip)}
                      controls
                      style={{ width: '100%', height: '100%' }}
                    />
                  </div>
                  {clip.hook && (
                    <p style={{ fontSize: '0.8rem', color: 'var(--secondary)', fontStyle: 'italic', marginBottom: '0.3rem' }}>
                      "{clip.hook}"
                    </p>
                  )}
                  <p style={{ fontSize: '0.9rem' }}>{clip.description}</p>
                  {(clip.virality_score || clip.clip_type) && (
                    <div style={{ display: 'flex', gap: '0.5rem', marginTop: '0.5rem', flexWrap: 'wrap' }}>
                      {clip.virality_score && (
                        <span className="clip-badge">{clip.virality_score}/10</span>
                      )}
                      {clip.clip_type && (
                        <span className="clip-badge clip-badge-type">{clip.clip_type}</span>
                      )}
                    </div>
                  )}
                  <div style={{ display: 'flex', gap: '0.5rem', marginTop: '1rem' }}>
                    <button
                      className="btn btn-secondary"
                      style={{ flex: 1, fontSize: '0.85rem', padding: '0.6rem' }}
                      onClick={() => openTrimmer(clip)}
                    >
                      Edit
                    </button>
                    <a
                      href={getClipUrl(clip)}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="btn btn-primary"
                      style={{ flex: 1, textDecoration: 'none', textAlign: 'center', fontSize: '0.85rem', padding: '0.6rem' }}
                    >
                      Download
                    </a>
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}
      </div>

      <section className="features">
        <div className="feature-card">
          <div className="icon">AI</div>
          <h3>AI Analysis</h3>
          <p>Our clipping engine understands hooks, sentiment, and key moments in your video.</p>
        </div>
        <div className="feature-card">
          <div className="icon">✂️</div>
          <h3>Auto Clipping</h3>
          <p>Precise cuts and vertical (9:16) cropping optimized for Reels, TikTok, and Shorts.</p>
        </div>
        <div className="feature-card">
          <div className="icon">⚡</div>
          <h3>Instant Results</h3>
          <p>Get viral-ready clips in minutes, ready for download and sharing.</p>
        </div>
      </section>

      <footer style={{ padding: '4rem', textAlign: 'center', color: 'var(--text-secondary)' }}>
        <p>&copy; 2026 Clipwave AI. Built with ❤️ for creators.</p>
      </footer>
    </main>
  );
}
