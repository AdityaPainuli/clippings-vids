"use client";

import { useState, useEffect } from "react";
import "./globals.css";

interface Clip {
  url?: string;
  path?: string; // keeping just in case old cached data still has path
  src?: string;
  description?: string;
}

export default function Home() {
  const [url, setUrl] = useState("");
  const [instructions, setInstructions] = useState("");
  const [isProcessing, setIsProcessing] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<string>("");
  const [clips, setClips] = useState<Clip[]>([]);
  const [error, setError] = useState<string | null>(null);

  // --- Auth State ---
  const [token, setToken] = useState<string | null>(null);
  const [userEmail, setUserEmail] = useState<string | null>(null);

  const [showAuthModal, setShowAuthModal] = useState(false);
  const [authMode, setAuthMode] = useState<"login" | "signup">("login");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authError, setAuthError] = useState<string | null>(null);
  const [isAuthLoading, setIsAuthLoading] = useState(false);

  useEffect(() => {
    // Initialize auth from localStorage on mount
    const storedToken = localStorage.getItem("clipwave_access_token");
    const storedEmail = localStorage.getItem("clipwave_user_email");
    if (storedToken && storedEmail) {
      setToken(storedToken);
      setUserEmail(storedEmail);
    }
  }, []);

  const handleLogout = () => {
    localStorage.removeItem("clipwave_access_token");
    localStorage.removeItem("clipwave_user_email");
    setToken(null);
    setUserEmail(null);
    setClips([]);
    setJobId(null);
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
        // Signup success usually requires login afterwards or check email
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
    setClips([]);
    setStatus("Initiating...");

    try {
      const formData = new FormData();
      formData.append("url", url);
      if (instructions) formData.append("instructions", instructions);

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
    } catch (err: any) {
      setError(err.message);
      setIsProcessing(false);
    }
  };

  useEffect(() => {
    if (!jobId || !token) return;

    const interval = setInterval(async () => {
      try {
        const response = await fetch(`http://localhost:8000/status/${jobId}`, {
          headers: {
            "Authorization": `Bearer ${token}`
          }
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

    return () => clearInterval(interval);
  }, [jobId, token]);

  return (
    <main>
      <div className="glow-bg"></div>

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
            <div style={{ marginTop: '1rem', color: 'var(--secondary)' }}>
              {status}... This may take a few minutes as our clipping engine  analyzes the video.
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
                      src={clip.url || clip.src || (clip.path ? (clip.path.startsWith('http') ? clip.path : `http://localhost:8000${clip.path}`) : '')}
                      controls
                      style={{ width: '100%', height: '100%' }}
                    />
                  </div>
                  <p style={{ fontSize: '0.9rem' }}>{clip.description}</p>
                  <a
                    href={clip.url || clip.src || (clip.path ? (clip.path.startsWith('http') ? clip.path : `http://localhost:8000${clip.path}`) : '#')}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="btn btn-primary"
                    style={{ marginTop: '1rem', display: 'inline-block', textDecoration: 'none' }}
                  >
                    Download
                  </a>
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
