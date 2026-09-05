import assert from "node:assert/strict";
import test from "node:test";

import { buildManifest, bumpVersion, parseReleaseCommit, validateCommitClassification } from "./generate-release-notes.mjs";

const config = {
  base_version: "1.0.0",
  base_commit: "base",
  repository_url: "https://github.com/vittok/stare",
  initial_release: {
    version: "1.0.0",
    title: "Initial",
    date: "2026-09-01",
    commit: "base",
    level: "major",
    changes: ["Initial release"]
  }
};

test("semantic versions distinguish patches, features, and breaking changes", () => {
  assert.equal(bumpVersion("1.0.0", "patch"), "1.0.1");
  assert.equal(bumpVersion("1.0.1", "minor"), "1.1.0");
  assert.equal(bumpVersion("1.1.0", "major"), "2.0.0");
});

test("release notes use explicit notes and ignore maintenance commits", () => {
  assert.deepEqual(
    parseReleaseCommit({
      subject: "feat(portal): add exports",
      body: "Release-Note: Download the current stock table as CSV."
    }),
    {
      level: "minor",
      title: "Add exports",
      changes: ["Download the current stock table as CSV."]
    }
  );
  assert.equal(parseReleaseCommit({ subject: "chore: update dependencies", body: "" }), null);
});

test("manifest applies release commits in chronological order", () => {
  const manifest = buildManifest(config, [
    { hash: "a", date: "2026-09-02T08:00:00Z", subject: "fix(portal): improve labels", body: "" },
    { hash: "b", date: "2026-09-03T08:00:00Z", subject: "feat(portal): add exports", body: "" },
    { hash: "c", date: "2026-09-04T08:00:00Z", subject: "fix(portal): preserve filters", body: "" }
  ]);

  assert.equal(manifest.current_version, "1.1.1");
  assert.deepEqual(manifest.releases.map((release) => release.version), ["1.1.1", "1.1.0", "1.0.1", "1.0.0"]);
});

test("unclassified commits cannot silently disappear from release history", () => {
  assert.throws(
    () => validateCommitClassification([{ hash: "abc1234", subject: "Change portal behavior", body: "" }]),
    /Commits must use a release type/
  );
  assert.doesNotThrow(() => validateCommitClassification([
    { hash: "a", subject: "Automated STARE daily update", body: "" },
    { hash: "b", subject: "docs: clarify deployment", body: "" },
    { hash: "c", subject: "internal cleanup", body: "Release-Note: none" }
  ]));
});
