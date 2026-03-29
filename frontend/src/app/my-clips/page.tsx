"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import "../globals.css";

interface PastClip {
  filename: string;
  job_id: string;
  storage_path: string;
  url: string;
  video_url: string;
  src: string;
  description: string;
  source_url: string;
  start_time: number;
  end_time: number;
  uploaded_at: string;
  expires_at: string;
  expires_in_seconds: number;
  expires_in_human: string;
  size_mb: number;
}

interface MyClipsResponse {
  total: number;
  ttl_hours: number;
  clips: PastClip[];
}

export default function MyClips() {
  const [clipsData, setClipsData] = useState<MyClipsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchClips = async () => {
      const token = localStorage.getItem("clipwave_access_token");
      if (!token) {
        setError("You must be logged in to view your clips.");
        setLoading(false);
        return;
      }

      try {
        const response = await fetch("http://localhost:8000/my-clips", {
          headers: {
            "Authorization": `Bearer ${token}`
          }
        });

        if (!response.ok) {
          if (response.status === 401 || response.status === 403) {
            localStorage.removeItem("clipwave_access_token");
            localStorage.removeItem("clipwave_user_email");
            setError("Session expired. Please return to the homepage to log in.");
          } else {
            throw new Error("Failed to fetch past clips.");
          }
          return;
        }

        const data: MyClipsResponse = await response.json();
        setClipsData(data);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchClips();
  }, []);

  return (
    <main>
      <div className="glow-bg"></div>

      <nav className="navbar">
        <Link href="/" style={{ textDecoration: 'none' }}>
          <div className="logo">Clipwave AI</div>
        </Link>
        <div className="action-buttons">
          <Link href="/">
            <button className="btn btn-secondary">Create New Clips</button>
          </Link>
        </div>
      </nav>

      <div style={{ padding: '6rem 2rem 2rem 2rem', maxWidth: '1200px', margin: '0 auto' }}>
        <h1 style={{ marginBottom: '1rem' }}>My Library</h1>
        
        {loading && <p style={{ color: 'var(--text-secondary)' }}>Loading your clips...</p>}
        
        {error && (
          <div style={{ padding: '2rem', background: 'rgba(255, 75, 75, 0.1)', border: '1px solid #ff4b4b', borderRadius: '12px', color: '#ff4b4b' }}>
            {error}
          </div>
        )}

        {!loading && !error && clipsData && (
          <>
            <p style={{ color: 'var(--text-secondary)', marginBottom: '3rem' }}>
              Showing {clipsData.total} active clips. Clips automatically expire after {clipsData.ttl_hours} hours.
            </p>

            {clipsData.clips.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '4rem', background: 'var(--glass-bg)', borderRadius: '16px', border: '1px solid var(--glass-border)' }}>
                <h2>No clips found</h2>
                <p style={{ color: 'var(--text-secondary)', marginTop: '1rem', marginBottom: '2rem' }}>
                  You don't have any clips or they have expired.
                </p>
                <Link href="/">
                  <button className="btn btn-primary">Create Clips</button>
                </Link>
              </div>
            ) : (
              <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
                gap: '2rem'
              }}>
                {clipsData.clips.map((clip, index) => (
                  <div key={index} className="feature-card" style={{ padding: '1rem', display: 'flex', flexDirection: 'column' }}>
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
                        src={clip.url || clip.src || clip.video_url}
                        controls
                        style={{ width: '100%', height: '100%' }}
                      />
                    </div>
                    
                    <div style={{ flexGrow: 1 }}>
                      <p style={{ fontSize: '0.9rem', marginBottom: '0.3rem', fontWeight: 'bold', wordBreak: 'break-all' }}>
                        {clip.filename}
                      </p>
                      {clip.description && (
                        <p style={{ fontSize: '0.85rem', color: '#ddd', marginBottom: '0.8rem', display: '-webkit-box', WebkitLineClamp: 3, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                          "{clip.description}"
                        </p>
                      )}
                      {clip.source_url && (
                        <div style={{ fontSize: '0.8rem', marginBottom: '0.8rem', wordBreak: 'break-all' }}>
                          <a href={clip.source_url} target="_blank" rel="noopener noreferrer" style={{ color: 'var(--primary)' }}>Source Video</a>
                          {(clip.start_time > 0 || clip.end_time > 0) && (
                            <span style={{ color: 'var(--text-secondary)', marginLeft: '4px' }}>
                              ({clip.start_time}s - {clip.end_time}s)
                            </span>
                          )}
                        </div>
                      )}
                      <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', display: 'flex', justifyContent: 'space-between', marginBottom: '0.2rem' }}>
                        <span>Expires in:</span>
                        <span style={{ color: clip.expires_in_seconds < 3600 ? '#ff4b4b' : 'inherit' }}>
                          {clip.expires_in_human}
                        </span>
                      </p>
                      <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', display: 'flex', justifyContent: 'space-between' }}>
                        <span>Size:</span>
                        <span>{clip.size_mb} MB</span>
                      </p>
                    </div>

                    <a
                      href={clip.url || clip.src || clip.video_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="btn btn-primary"
                      style={{ marginTop: '1rem', display: 'block', textAlign: 'center', textDecoration: 'none' }}
                    >
                      Download
                    </a>
                  </div>
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </main>
  );
}
