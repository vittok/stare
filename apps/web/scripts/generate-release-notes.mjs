import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const WEB_ROOT = resolve(SCRIPT_DIR, "..");

export function bumpVersion(version, level) {
  let [major, minor, patch] = version.split(".").map(Number);
  if (level === "major") [major, minor, patch] = [major + 1, 0, 0];
  else if (level === "minor") [minor, patch] = [minor + 1, 0];
  else patch += 1;
  return `${major}.${minor}.${patch}`;
}

export function parseReleaseCommit(commit) {
  const match = commit.subject.match(/^(feat|fix|perf|revert)(?:\(([^)]+)\))?(!)?:\s+(.+)$/i);
  const breaking = Boolean(match?.[3]) || /(^|\n)BREAKING[ -]CHANGE:/i.test(commit.body);
  if (!match && !breaking) return null;

  const type = match?.[1].toLowerCase() || "feat";
  const summary = (match?.[4] || commit.subject).trim();
  const explicitNotes = commit.body
    .split("\n")
    .map((line) => line.match(/^Release-Note:\s*(.+)$/i)?.[1]?.trim())
    .filter(Boolean);
  const fallbackNote = summary.charAt(0).toUpperCase() + summary.slice(1);

  return {
    level: breaking ? "major" : type === "feat" ? "minor" : "patch",
    title: fallbackNote,
    changes: explicitNotes.length ? explicitNotes : [fallbackNote]
  };
}

export function isIgnoredCommit(commit) {
  return /^Automated STARE daily update$/i.test(commit.subject)
    || /^Merge\b/i.test(commit.subject)
    || /^(build|chore|ci|docs|refactor|style|test)(?:\([^)]+\))?:/i.test(commit.subject)
    || /(^|\n)Release-Note:\s*none\s*($|\n)/i.test(commit.body);
}

export function buildManifest(config, commits) {
  let version = config.base_version;
  const releases = [
    {
      ...config.initial_release,
      commit_url: `${config.repository_url}/commit/${config.initial_release.commit}`
    }
  ];

  for (const commit of commits) {
    const parsed = parseReleaseCommit(commit);
    if (!parsed) continue;
    version = bumpVersion(version, parsed.level);
    releases.push({
      version,
      title: parsed.title,
      date: commit.date.slice(0, 10),
      commit: commit.hash,
      commit_url: `${config.repository_url}/commit/${commit.hash}`,
      level: parsed.level,
      changes: parsed.changes
    });
  }

  return {
    current_version: version,
    current_commit: commits.at(-1)?.hash || config.base_commit,
    releases: releases.reverse()
  };
}

export function validateCommitClassification(commits) {
  const unclassified = commits.filter((commit) => !parseReleaseCommit(commit) && !isIgnoredCommit(commit));
  if (!unclassified.length) return;
  const subjects = unclassified.map((commit) => `- ${commit.hash.slice(0, 7)} ${commit.subject}`).join("\n");
  throw new Error(
    `Commits must use a release type or an explicit non-release type:\n${subjects}\n`
    + "Use feat/fix/perf/revert for user-facing work, or a maintenance type/Release-Note: none otherwise."
  );
}

function readLocalCommits(baseCommit) {
  const output = execFileSync(
    "git",
    ["log", "--reverse", "--format=%H%x1f%cI%x1f%s%x1f%b%x1e", `${baseCommit}..HEAD`],
    { cwd: WEB_ROOT, encoding: "utf8" }
  );
  return output
    .split("\x1e")
    .map((record) => record.trim())
    .filter(Boolean)
    .map((record) => {
      const [hash, date, subject, body = ""] = record.split("\x1f");
      return { hash, date, subject, body };
    });
}

async function readRemoteCommits(config) {
  let head = process.env.RENDER_GIT_COMMIT;
  if (!head) {
    try {
      head = execFileSync("git", ["rev-parse", "HEAD"], { cwd: WEB_ROOT, encoding: "utf8" }).trim();
    } catch {
      head = "main";
    }
  }
  const repository = new URL(config.repository_url).pathname.replace(/^\//, "");
  const items = [];
  for (let page = 1; ; page += 1) {
    const response = await fetch(`https://api.github.com/repos/${repository}/compare/${config.base_commit}...${head}?per_page=100&page=${page}`, {
      headers: { accept: "application/vnd.github+json", "user-agent": "stare-release-generator" }
    });
    if (!response.ok) throw new Error(`GitHub compare request returned ${response.status}`);
    const payload = await response.json();
    items.push(...payload.commits);
    if (payload.commits.length < 100) break;
  }
  return items.map((item) => {
    const [subject, ...body] = item.commit.message.split("\n");
    return {
      hash: item.sha,
      date: item.commit.author?.date || item.commit.committer?.date,
      subject,
      body: body.join("\n").trim()
    };
  });
}

export async function generateReleaseNotes() {
  const config = JSON.parse(readFileSync(resolve(WEB_ROOT, "release-baseline.json"), "utf8"));
  let commits = [];
  try {
    commits = readLocalCommits(config.base_commit);
  } catch (localError) {
    try {
      commits = await readRemoteCommits(config);
    } catch (remoteError) {
      throw new Error(`Release history could not read local or remote Git metadata: ${localError.message}; ${remoteError.message}`);
    }
  }
  validateCommitClassification(commits);
  const manifest = buildManifest(config, commits);
  writeFileSync(resolve(WEB_ROOT, "public/releases.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  console.log(`Generated S.T.A.R.E v${manifest.current_version} with ${manifest.releases.length} release entries.`);
  return manifest;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  await generateReleaseNotes();
}
