"use client";

import { Sparkles } from "lucide-react";
import { useEffect, useRef, useState } from "react";

type Release = {
  version: string;
  title: string;
  date: string;
  commit: string;
  commit_url: string;
  level: "major" | "minor" | "patch";
  changes: string[];
};

type ReleaseManifest = {
  current_version: string;
  current_commit: string;
  releases: Release[];
};

const SEEN_VERSION_KEY = "stare-seen-release-version";

export function PortalUpdates() {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const [manifest, setManifest] = useState<ReleaseManifest | null>(null);
  const [unread, setUnread] = useState(false);

  useEffect(() => {
    let cancelled = false;
    void fetch("/releases.json", { cache: "no-store" })
      .then((response) => response.ok ? response.json() as Promise<ReleaseManifest> : null)
      .then((data) => {
        if (cancelled || !data) return;
        setManifest(data);
        setUnread(window.localStorage.getItem(SEEN_VERSION_KEY) !== data.current_version);
      })
      .catch(() => undefined);
    return () => { cancelled = true; };
  }, []);

  function openUpdates() {
    dialogRef.current?.showModal();
    if (manifest) window.localStorage.setItem(SEEN_VERSION_KEY, manifest.current_version);
    setUnread(false);
  }

  function closeUpdates() {
    dialogRef.current?.close();
  }

  return (
    <>
      <button
        aria-label={unread ? "Open What's new, new updates available" : "Open What's new"}
        className="portal-tool-button updates-launcher"
        onClick={openUpdates}
        ref={triggerRef}
        title={unread ? "What's new - new updates available" : "What's new"}
        type="button"
      >
        <Sparkles aria-hidden="true" size={16} strokeWidth={2} />
        <span>What&apos;s new</span>
        {unread ? <i aria-hidden="true" className="update-indicator" /> : null}
      </button>

      <dialog
        aria-labelledby="portal-updates-title"
        className="help-dialog updates-dialog"
        onClick={(event) => event.target === event.currentTarget && closeUpdates()}
        onClose={() => triggerRef.current?.focus()}
        ref={dialogRef}
      >
        <div className="help-dialog-header">
          <div>
            <p className="eyebrow">Release notes</p>
            <h2 id="portal-updates-title">What&apos;s new</h2>
          </div>
          <button aria-label="Close What's new" className="help-close" onClick={closeUpdates} title="Close" type="button">&times;</button>
        </div>

        <div className="release-list">
          {manifest?.releases.length ? manifest.releases.map((release, index) => (
            <article className="release-entry" key={`${release.version}-${release.commit}`}>
              <div className="release-heading">
                <div><span>v{release.version}</span>{index === 0 ? <b>Current</b> : null}</div>
                <time dateTime={release.date}>{new Intl.DateTimeFormat("en-GB", { dateStyle: "medium", timeZone: "UTC" }).format(new Date(`${release.date}T00:00:00Z`))}</time>
              </div>
              <h3>{release.title}</h3>
              <ul>{release.changes.map((change) => <li key={change}>{change}</li>)}</ul>
              <a href={release.commit_url} rel="noreferrer" target="_blank">Commit {release.commit.slice(0, 7)}</a>
            </article>
          )) : <p className="release-empty">Release history is unavailable right now.</p>}
        </div>
      </dialog>
    </>
  );
}
