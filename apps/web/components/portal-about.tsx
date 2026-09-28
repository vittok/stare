"use client";

import { Info } from "lucide-react";
import { useEffect, useRef, useState } from "react";

type PortalAboutProps = {
  marketDataDate?: string | null;
  portalUpdated?: string | null;
};

type CurrentRelease = {
  date: string;
  version: string;
};

type ReleaseManifest = {
  current_version: string;
  releases: CurrentRelease[];
};

function formatDate(value?: string | null) {
  if (!value) return "Not available";
  const date = new Date(value.length === 10 ? `${value}T00:00:00Z` : value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeZone: "UTC"
  }).format(date);
}

function formatTimestamp(value?: string | null) {
  if (!value) return "Not available";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "UTC"
  }).format(date);
}

export function PortalAbout({ marketDataDate, portalUpdated }: PortalAboutProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const [release, setRelease] = useState<CurrentRelease | null>(null);

  useEffect(() => {
    let cancelled = false;
    void fetch("/releases.json", { cache: "no-store" })
      .then((response) => response.ok ? response.json() as Promise<ReleaseManifest> : null)
      .then((manifest) => {
        if (cancelled || !manifest) return;
        const current = manifest.releases.find((item) => item.version === manifest.current_version);
        setRelease(current ?? { version: manifest.current_version, date: "" });
      })
      .catch(() => undefined);
    return () => { cancelled = true; };
  }, []);

  function openAbout() {
    dialogRef.current?.showModal();
  }

  function closeAbout() {
    dialogRef.current?.close();
  }

  return (
    <>
      <button
        aria-label="Open About S.T.A.R.E"
        className="portal-tool-button"
        onClick={openAbout}
        ref={triggerRef}
        title="About"
        type="button"
      >
        <Info aria-hidden="true" size={17} strokeWidth={2} />
        <span>About</span>
      </button>

      <dialog
        aria-labelledby="portal-about-title"
        className="help-dialog about-dialog"
        onClick={(event) => event.target === event.currentTarget && closeAbout()}
        onClose={() => triggerRef.current?.focus()}
        ref={dialogRef}
      >
        <div className="help-dialog-header">
          <div>
            <p className="eyebrow">Product information</p>
            <h2 id="portal-about-title">About S.T.A.R.E</h2>
          </div>
          <button aria-label="Close About" className="help-close" onClick={closeAbout} title="Close" type="button">&times;</button>
        </div>

        <div className="about-content">
          <div className="about-release" aria-label="Current portal release">
            <span>Current release</span>
            <strong>{release ? `v${release.version}` : "Version unavailable"}</strong>
            <time dateTime={release?.date || undefined}>{release?.date ? `Released ${formatDate(release.date)}` : "Release date unavailable"}</time>
          </div>

          <section>
            <p className="eyebrow">Portal snapshot</p>
            <dl className="about-facts">
              <div><dt>Portal updated</dt><dd>{portalUpdated ? `${formatTimestamp(portalUpdated)} UTC` : "Not available"}</dd></div>
              <div><dt>Market data</dt><dd>{formatDate(marketDataDate)}</dd></div>
            </dl>
          </section>

          <section>
            <p className="eyebrow">Purpose</p>
            <p>S.T.A.R.E—Stock Trend Analysis Risk Engine—organizes market direction, activity, fundamentals, and deterministic stock signals into a repeatable research workflow.</p>
          </section>

          <section>
            <p className="eyebrow">Data and signals</p>
            <p>Market and fundamental data is sourced from Yahoo Finance. Prices represent the latest close captured by the scheduled update and are not real-time quotes. Signals may be incomplete or delayed and are research outputs, not personalized financial advice.</p>
          </section>

          <section>
            <p className="eyebrow">Project</p>
            <p>Created by vittok. The authenticated portal is hosted on Render, with the public GitHub Pages report available as a read-only fallback.</p>
            <div className="about-links">
              <a href="https://vittok.github.io/stare/" rel="noreferrer" target="_blank">Open read-only report</a>
              <a href="https://github.com/vittok/stare" rel="noreferrer" target="_blank">View project on GitHub</a>
            </div>
          </section>
        </div>
      </dialog>
    </>
  );
}
