import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import {
  ArrowLeft, BarChart3, BellRing, BookOpen, CheckCircle2, Clock3, Database,
  Download, History, ListFilter, Search, Settings2, ShieldCheck, Sparkles, Star, TrendingUp
} from "lucide-react";

export const metadata: Metadata = {
  title: "Help & User Guide | S.T.A.R.E",
  description: "Learn how to scan markets, interpret signals, compare stocks, personalize research, and review historical data in S.T.A.R.E."
};

const sections = [
  ["quick-start", "Daily workflow"], ["navigate", "Find your market"],
  ["market-signals", "Read market signals"], ["stock-research", "Research a stock"],
  ["personalize", "Personalize your view"], ["history-export", "History and exports"],
  ["data-freshness", "Updates and freshness"], ["field-guide", "Field guide"],
  ["account-privacy", "Account and privacy"], ["troubleshooting", "Troubleshooting"]
] as const;

const workflow = [
  ["Check freshness", "Start with the update timestamp, market-data date, and any alert. This tells you whether the snapshot represents the latest expected trading session.", Clock3],
  ["Choose a market", "Select All Regions for a broad scan, NA/Sectors for North American sectors, or APAC, EMEA, and LAC for country and market views.", ListFilter],
  ["Read direction first", "Use Bullish, Bearish, or Neutral together with Strength. Direction tells you the side of the move; strength tells you how pronounced it is.", TrendingUp],
  ["Shortlist stocks", "Use Top Active Stocks for attention and liquidity, Top Picks for a fundamentals-first ranking, and the table for deeper comparison.", Star],
  ["Open the evidence", "Select a ticker or company name to compare price context, fundamentals, the standard signal, your personal signal, confidence, and rationale.", Search]
] as const;

function GuideSection({ children, eyebrow, id, title }: { children: React.ReactNode; eyebrow: string; id: string; title: string }) {
  return <section className="guide-section" id={id}><div className="guide-section-heading"><p className="eyebrow">{eyebrow}</p><h2>{title}</h2></div>{children}</section>;
}

export default function HelpPage() {
  return (
    <main className="help-page">
      <header className="help-topbar">
        <Link className="help-brand" href="/" aria-label="Return to S.T.A.R.E portal"><Image alt="S.T.A.R.E logo" height={44} priority src="/Logo.png" width={44} /><span>Stock Trend Analysis Risk Engine <b>(S.T.A.R.E)</b></span></Link>
        <Link className="button secondary help-back" href="/"><ArrowLeft aria-hidden="true" size={16} /> Return to portal</Link>
      </header>

      <div className="help-hero">
        <div><p className="eyebrow">Help center · User guide</p><h1>Make every market review a repeatable workflow.</h1><p>Start with a five-step daily scan, then use the detailed guide when you need to understand a signal, configure your workspace, or investigate a problem.</p></div>
        <div className="help-hero-note"><BookOpen aria-hidden="true" size={24} /><div><strong>Best place to begin</strong><span>Follow Daily workflow from top to bottom. It mirrors the order in which the portal presents the evidence.</span></div></div>
      </div>

      <div className="help-layout">
        <aside className="help-nav" aria-label="Help topics">
          <p>On this page</p>
          <nav>{sections.map(([href, label], index) => <a href={`#${href}`} key={href}><span>{String(index + 1).padStart(2, "0")}</span>{label}</a>)}</nav>
          <div className="help-nav-notice"><ShieldCheck aria-hidden="true" size={18} /><p>S.T.A.R.E is a research and screening tool—not personalized financial advice or a promise of future performance.</p></div>
        </aside>

        <article className="help-guide">
          <GuideSection eyebrow="Start here" id="quick-start" title="Your daily five-step workflow">
            <div className="workflow-list">{workflow.map(([title, copy, Icon], index) => <div className="workflow-step" key={title}><span className="workflow-number">{index + 1}</span><Icon aria-hidden="true" size={20} /><div><h3>{title}</h3><p>{copy}</p></div></div>)}</div>
            <div className="guide-callout success"><CheckCircle2 aria-hidden="true" size={19} /><p><strong>A good review ends with evidence, not a label.</strong> Treat Buy, Hold, and Sell as screening outputs. Open the stock and check the rationale, source date, valuation, momentum, and wider market context before acting.</p></div>
          </GuideSection>

          <GuideSection eyebrow="Navigation" id="navigate" title="Find the market view you need">
            <div className="guide-grid two">
              <div className="guide-card"><strong>Search</strong><p>Filters the current report by ticker, company, sector, country, or market. Search narrows what is already in the selected snapshot.</p></div>
              <div className="guide-card"><strong>Analyze any stock</strong><p>Looks up a ticker or company beyond the scheduled report, loads current fundamentals and model signals, and lets signed-in users save it to the active watchlist.</p></div>
              <div className="guide-card"><strong>Direction</strong><p>Show all groups or narrow the workspace to Bullish, Bearish, or Neutral market context.</p></div>
              <div className="guide-card"><strong>Regions</strong><p>All Regions provides the broad scan. NA/Sectors reveals North American sectors. APAC, EMEA, and LAC reveal their covered countries and markets.</p></div>
            </div>
            <h3 className="guide-subheading">Snapshot versus History</h3>
            <div className="guide-comparison"><div><BarChart3 aria-hidden="true" size={20} /><strong>Snapshot</strong><p>The newest market view: strength map, active names, top picks, filters, and the complete stock table.</p></div><div><History aria-hidden="true" size={20} /><strong>History</strong><p>Time-series context for regions, sectors, prices, returns, activity, volume, and recommendation changes.</p></div></div>
          </GuideSection>

          <GuideSection eyebrow="Interpretation" id="market-signals" title="Read the market layer before the stock layer">
            <dl className="guide-definition-list">
              <div><dt>Direction</dt><dd><b>Bullish</b> means the blended market score is positive, <b>Bearish</b> means it is negative, and <b>Neutral</b> means the score is close to zero.</dd></div>
              <div><dt>Strength</dt><dd>A 0–100 intensity measure based on breadth, median weekly return, and unusual trading volume. It measures how pronounced the direction is—not the probability that it will continue.</dd></div>
              <div><dt>Strength map</dt><dd>Ranks regions or sectors by strength. Green, red, and amber encode Bullish, Bearish, and Neutral direction. Selectable sector bars also change the active sector filter.</dd></div>
              <div><dt>KPIs</dt><dd>Summarize the filtered workspace: group count, Bullish versus Bearish balance, average strength, and distinct tracked stocks.</dd></div>
            </dl>
            <div className="guide-formula"><p className="eyebrow">How sector strength is built</p><div><span><b>50%</b> market breadth</span><span><b>35%</b> median weekly return</span><span><b>15%</b> abnormal volume</span></div><p>Breadth has the largest influence, returns provide direction and magnitude, and volume acts as confirmation.</p></div>
          </GuideSection>

          <GuideSection eyebrow="Stock research" id="stock-research" title="Move from shortlist to evidence">
            <div className="guide-grid three">
              <div className="guide-card"><Sparkles aria-hidden="true" size={19} /><strong>Top Active Stocks</strong><p>Ranks names by latest-session dollar volume. It identifies where trading attention is concentrated; high activity is not automatically a positive signal.</p></div>
              <div className="guide-card"><Star aria-hidden="true" size={19} /><strong>Top Picks</strong><p>Uses P/E, P/B, PEG, and dividend yield first, then adds bounded daily price and activity context. The ranking is separate from Buy/Hold/Sell.</p></div>
              <div className="guide-card"><Database aria-hidden="true" size={19} /><strong>Complete snapshot</strong><p>Provides the sortable evidence table. Use Columns to reduce noise and select a ticker or company to open its detail view.</p></div>
            </div>
            <h3 className="guide-subheading">Inside a stock detail</h3>
            <ol className="guide-steps">
              <li><b>Confirm the instrument and source market.</b> Check ticker, company, exchange, currency, and the price date.</li>
              <li><b>Review price and activity.</b> Compare the latest close, prior close, weekly return, latest volume, dollar volume, and volume ratio.</li>
              <li><b>Review fundamentals.</b> Use valuation, growth, profitability, balance-sheet, dividend, beta, and 52-week context together.</li>
              <li><b>Compare model outputs.</b> Standard is the shared model. Personal applies your saved factor multipliers to the same snapshot.</li>
              <li><b>Read confidence and rationale.</b> Confidence reflects the magnitude of the model score. It is not a statistical probability of profit.</li>
            </ol>
          </GuideSection>

          <GuideSection eyebrow="Your workspace" id="personalize" title="Save the view that supports your process">
            <div className="guide-grid two">
              <div className="guide-card"><Star aria-hidden="true" size={19} /><strong>Named watchlists</strong><p>Create lists for different ideas, select the active list, and use the star beside a ticker to add or remove it. The watchlist button above the table opens all its saved symbols.</p></div>
              <div className="guide-card"><Settings2 aria-hidden="true" size={19} /><strong>Scoring weights</strong><p>Adjust market sentiment, P/E, P/B, PEG, dividend, and momentum from 0.0× to 2.0×. Save applies the weights; Reset restores the standard 1.0× model.</p></div>
              <div className="guide-card"><BellRing aria-hidden="true" size={19} /><strong>Email reports</strong><p>Choose market-close only or every update, then receive either the full market report or your default watchlist report at the verified account email.</p></div>
              <div className="guide-card"><ListFilter aria-hidden="true" size={19} /><strong>Saved preferences</strong><p>The portal remembers filters, region or sector choices, visible columns, theme, and other supported workspace settings for your next visit.</p></div>
            </div>
            <div className="guide-callout"><Settings2 aria-hidden="true" size={19} /><p><strong>Weights change emphasis, not source data.</strong> A personal signal recalculates factor contributions but does not change prices, fundamentals, market direction, or the stored standard signal.</p></div>
          </GuideSection>

          <GuideSection eyebrow="Analysis tools" id="history-export" title="Add time context and take results with you">
            <div className="guide-grid two">
              <div className="guide-card"><History aria-hidden="true" size={19} /><strong>Group history</strong><p>Chart region and sector strength across the retained period. Multiple observations on one date can represent separate updates, such as market open and close.</p></div>
              <div className="guide-card"><TrendingUp aria-hidden="true" size={19} /><strong>Ticker Compare</strong><p>Place up to five stocks on common price, return, activity, volume, and recommendation timelines.</p></div>
              <div className="guide-card"><Download aria-hidden="true" size={19} /><strong>CSV</strong><p>Best for spreadsheets. Snapshot exports follow current filters; history exports follow the active period and selected groups or tickers.</p></div>
              <div className="guide-card"><Download aria-hidden="true" size={19} /><strong>JSON</strong><p>Best when you need report structure, nested metadata, or machine-readable fields instead of flat rows.</p></div>
            </div>
          </GuideSection>

          <GuideSection eyebrow="Data operations" id="data-freshness" title="Know what was updated—and when">
            <dl className="guide-definition-list">
              <div><dt>Portal updated</dt><dd>When the report was processed and published.</dd></div>
              <div><dt>Market-data date</dt><dd>The trading date represented by the prices. Use this when judging price freshness.</dd></div>
              <div><dt>Refresh data</dt><dd>Authorized users can request an update. Progress appears in the portal, and an update already running is reused instead of duplicated.</dd></div>
              <div><dt>Freshness alert</dt><dd>Appears when an update failed, completed partially, or the latest successful data is older than expected. The portal keeps showing the last valid snapshot.</dd></div>
            </dl>
            <div className="guide-callout warning"><Clock3 aria-hidden="true" size={19} /><p>Weekends, market holidays, early closes, and upstream source delays can make the portal-update time newer than the market-data date without indicating a problem.</p></div>
          </GuideSection>

          <GuideSection eyebrow="Reference" id="field-guide" title="Field guide">
            <div className="field-table-wrap"><table className="field-table"><thead><tr><th>Field</th><th>What it means</th><th>Use it carefully</th></tr></thead><tbody>
              <tr><td>Buy / Hold / Sell</td><td>Deterministic score combining market context, momentum, valuation, growth/value support, and income.</td><td>A screening label, not a trade instruction.</td></tr>
              <tr><td>Confidence</td><td>How far the computed score sits from neutral.</td><td>Not a probability, expected return, or risk estimate.</td></tr>
              <tr><td>P/E</td><td>Share price relative to earnings.</td><td>Negative, unusually high, or missing values need business context.</td></tr>
              <tr><td>P/B</td><td>Market value relative to book value.</td><td>Comparability varies significantly by industry.</td></tr>
              <tr><td>PEG</td><td>P/E considered alongside expected growth.</td><td>Depends on the reliability of growth estimates.</td></tr>
              <tr><td>Dividend yield</td><td>Annual dividend relative to share price.</td><td>A high yield may be unsustainable or reflect price weakness.</td></tr>
              <tr><td>Weekly return</td><td>Recent price change over the weekly calculation window.</td><td>Past momentum can reverse.</td></tr>
              <tr><td>Volume ratio</td><td>Current five-session volume versus the prior eight-week weekly average.</td><td>Shows unusual activity, not whether it is informed or positive.</td></tr>
              <tr><td>Dollar volume</td><td>Latest close multiplied by latest-session volume.</td><td>Measures trading activity, not company size or quality.</td></tr>
              <tr><td>Activity percentile</td><td>Relative latest-session activity within the comparison set.</td><td>The comparison set changes with the active market view.</td></tr>
            </tbody></table></div>
          </GuideSection>

          <GuideSection eyebrow="Your data" id="account-privacy" title="Account, privacy, and access">
            <div className="guide-grid two">
              <div className="guide-card"><ShieldCheck aria-hidden="true" size={19} /><strong>What is stored</strong><p>Your authenticated identity and email, portal preferences, named watchlists, scoring weights, and email-report settings provide your personal workspace.</p></div>
              <div className="guide-card"><ShieldCheck aria-hidden="true" size={19} /><strong>How it is separated</strong><p>User-owned data is protected with per-user access controls. Signing out removes access from the current browser session.</p></div>
              <div className="guide-card"><BookOpen aria-hidden="true" size={19} /><strong>Read-only access</strong><p>The login screen links to the public GitHub Pages report when you want the overview without signing into the personalized portal.</p></div>
              <div className="guide-card"><BellRing aria-hidden="true" size={19} /><strong>Email control</strong><p>Email reports are opt-in. You choose their frequency and scope and can turn them off from sidebar settings.</p></div>
            </div>
          </GuideSection>

          <GuideSection eyebrow="When something looks wrong" id="troubleshooting" title="Troubleshooting">
            <div className="faq-list">
              <details><summary>The portal takes time to load after being idle.</summary><p>The hosting service may be waking a free-tier portal or API instance. Keep the page open briefly; the portal automatically contacts the API. Use Retry now if loading remains.</p></details>
              <details><summary>Google sign-in returns me to the login screen.</summary><p>Retry after the portal is fully awake. If it persists, allow cookies for the portal, confirm you used an approved account, and report the time and browser used.</p></details>
              <details><summary>The displayed price is not the live market price.</summary><p>S.T.A.R.E displays the latest close captured by its scheduled update, not a real-time quote. Check the market-data date and price date.</p></details>
              <details><summary>My personal signal differs from the standard signal.</summary><p>This is expected when saved weights move the score across the Buy or Sell threshold. Open the detail to compare both scores and rationales.</p></details>
              <details><summary>A saved ticker is not in the main report.</summary><p>On-demand stocks can remain in a watchlist outside the scheduled universe. Open the active watchlist or select the saved symbol under Analyze any stock.</p></details>
              <details><summary>An update alert is visible.</summary><p>Read the alert and use the snapshot as historical context. The portal preserves the last valid report when a newer update fails or is incomplete.</p></details>
            </div>
          </GuideSection>

          <footer className="help-footer"><div><strong>Ready to use the workflow?</strong><p>Return to the portal and begin with the freshness information in the current snapshot.</p></div><Link className="button" href="/">Open S.T.A.R.E</Link></footer>
        </article>
      </div>
    </main>
  );
}
