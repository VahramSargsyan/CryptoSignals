const crypto = require("node:crypto");

const DEFAULT_REPOSITORY = "VahramSargsyan/CryptoSignals";
const DEFAULT_MONITOR_WORKFLOW = "relative-rotation-paper-live-v1.yml";

const SCHEDULE_MODES = new Map([
  ["30 6 * * *", "morning"],
  ["30 18 * * *", "evening"]
]);

function requiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function safeEqual(left, right) {
  const a = Buffer.from(String(left || ""));
  const b = Buffer.from(String(right || ""));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function validCronSecret(request) {
  const expected = String(process.env.CRON_SECRET || "").trim();
  if (!expected) return false;
  const actual = String(request.headers?.authorization || "").trim();
  return safeEqual(actual, `Bearer ${expected}`);
}

function resolveNotificationMode(request) {
  const schedule = String(
    request.headers?.["x-vercel-cron-schedule"] || ""
  ).trim();
  if (SCHEDULE_MODES.has(schedule)) {
    return SCHEDULE_MODES.get(schedule);
  }

  const rawMode = Array.isArray(request.query?.mode)
    ? request.query.mode[0]
    : request.query?.mode;
  const mode = String(rawMode || "").trim().toLowerCase();
  if (mode === "morning" || mode === "evening") {
    return mode;
  }

  throw new Error("Unknown RR scheduler invocation");
}

async function dispatchMonitor(mode) {
  const token = requiredEnv("GITHUB_DISPATCH_TOKEN");
  const repository = String(
    process.env.GITHUB_REPOSITORY || DEFAULT_REPOSITORY
  ).trim();
  const workflow = String(
    process.env.GITHUB_MONITOR_WORKFLOW || DEFAULT_MONITOR_WORKFLOW
  ).trim();

  const response = await fetch(
    `https://api.github.com/repos/${repository}/actions/workflows/${encodeURIComponent(workflow)}/dispatches`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "x-github-api-version": "2022-11-28",
        "content-type": "application/json",
        "user-agent": "rr-vercel-scheduler"
      },
      body: JSON.stringify({
        ref: "main",
        inputs: {
          notification_mode: mode
        }
      })
    }
  );

  if (![200, 204].includes(response.status)) {
    const body = await response.text();
    throw new Error(
      `GitHub monitor workflow dispatch failed: HTTP ${response.status} ${body}`
    );
  }
}

async function handler(request, response) {
  if (request.method !== "GET") {
    response.setHeader("allow", "GET");
    response.status(405).json({ ok: false, error: "method_not_allowed" });
    return;
  }

  if (!validCronSecret(request)) {
    response.status(401).json({ ok: false, error: "invalid_cron_secret" });
    return;
  }

  try {
    const mode = resolveNotificationMode(request);
    await dispatchMonitor(mode);
    response.status(200).json({
      ok: true,
      service: "rr-vercel-scheduler",
      notification_mode: mode,
      dispatched: true
    });
  } catch (error) {
    console.error(error);
    response.status(502).json({ ok: false, error: "scheduler_dispatch_failed" });
  }
}

module.exports = handler;
module.exports._test = {
  SCHEDULE_MODES,
  safeEqual,
  validCronSecret,
  resolveNotificationMode,
  dispatchMonitor
};
