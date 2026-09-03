"use client";

import { useRef } from "react";

export function PortalHelp() {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  function openHelp() {
    dialogRef.current?.showModal();
  }

  function closeHelp() {
    dialogRef.current?.close();
  }

  return (
    <>
      <button
        aria-label="Open portal help"
        className="help-launcher"
        onClick={openHelp}
        ref={triggerRef}
        title="Help"
        type="button"
      >
        ?
      </button>

      <dialog
        aria-labelledby="portal-help-title"
        className="help-dialog"
        onClick={(event) => event.target === event.currentTarget && closeHelp()}
        onClose={() => triggerRef.current?.focus()}
        ref={dialogRef}
      >
        <div className="help-dialog-header">
          <div>
            <p className="eyebrow">User guide</p>
            <h2 id="portal-help-title">How S.T.A.R.E works</h2>
          </div>
          <button aria-label="Close help" className="help-close" onClick={closeHelp} title="Close" type="button">&times;</button>
        </div>

        <div className="help-content">
          <section>
            <h3>Choose your market view</h3>
            <p>Use the left sidebar to view all regions, North America, North American sectors, or an international region. Selecting NA/Sectors reveals individual sectors; selecting LAC, EMEA, or APAC reveals the covered countries and markets.</p>
          </section>
          <section>
            <h3>Read direction and strength</h3>
            <p>Bullish, Bearish, and Neutral describe the recent direction of a region or sector. Strength is a 0-100 measure of how pronounced that direction is, based on market breadth, returns, and trading activity. It is an intensity measure, not a probability or forecast guarantee.</p>
          </section>
          <section>
            <h3>Understand the stock signal</h3>
            <p>Buy, Hold, and Sell combine the surrounding market direction with price momentum and company fundamentals such as P/E, P/B, PEG, and dividend yield. Select a ticker or a Decision Snapshot label to see the rationale behind an individual result.</p>
          </section>
          <section>
            <h3>Use Top Active Stocks</h3>
            <p>Top Active Stocks highlights names with the highest dollar trading volume during the latest available trading day. High activity makes a stock noteworthy for review, but does not by itself make the stock attractive to buy.</p>
          </section>
          <section>
            <h3>Explore and compare</h3>
            <p>Search by ticker, company, sector, country, or market. Select table headers marked with sorting arrows to reorder results, and use Columns to keep only the information useful to your review. Select a company name for a concise business and financial profile.</p>
          </section>
          <section>
            <h3>Save your view</h3>
            <p>Add companies to your watchlist and choose your preferred theme, region, sector, market, and visible columns. These choices are saved to your signed-in profile and restored when you return.</p>
          </section>
          <section>
            <h3>Check freshness</h3>
            <p>The footer shows both when the portal was updated and the date represented by the market prices. These can differ on weekends, holidays, or when a source is delayed. Always use the market-data date when judging how current a price or signal is.</p>
          </section>
        </div>

        <p className="help-notice">S.T.A.R.E provides structured market research, not personalized financial advice. Consider your objectives, risk tolerance, and independent research before making a financial decision.</p>
      </dialog>
    </>
  );
}
