const test = require("node:test");
const assert = require("node:assert/strict");

const scheduler = require("../api/rr-scheduler.js")._test;

test("maps Vercel cron schedules to notification modes", () => {
  assert.equal(
    scheduler.resolveNotificationMode({
      headers: { "x-vercel-cron-schedule": "3 0 * * *" },
      query: {}
    }),
    "morning"
  );
  assert.equal(
    scheduler.resolveNotificationMode({
      headers: { "x-vercel-cron-schedule": "30 18 * * *" },
      query: {}
    }),
    "evening"
  );
});

test("allows explicit mode only as authenticated test fallback input", () => {
  assert.equal(
    scheduler.resolveNotificationMode({ headers: {}, query: { mode: "morning" } }),
    "morning"
  );
  assert.equal(
    scheduler.resolveNotificationMode({ headers: {}, query: { mode: "evening" } }),
    "evening"
  );
  assert.throws(() =>
    scheduler.resolveNotificationMode({ headers: {}, query: { mode: "other" } })
  );
});

test("validates bearer cron secret", () => {
  const old = process.env.CRON_SECRET;
  process.env.CRON_SECRET = "test-secret-123";
  try {
    assert.equal(
      scheduler.validCronSecret({
        headers: { authorization: "Bearer test-secret-123" }
      }),
      true
    );
    assert.equal(
      scheduler.validCronSecret({
        headers: { authorization: "Bearer wrong" }
      }),
      false
    );
  } finally {
    if (old === undefined) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = old;
  }
});

test("dispatches the monitor workflow with the requested mode", async () => {
  const oldFetch = global.fetch;
  const oldToken = process.env.GITHUB_DISPATCH_TOKEN;
  const oldRepo = process.env.GITHUB_REPOSITORY;
  const oldWorkflow = process.env.GITHUB_MONITOR_WORKFLOW;

  process.env.GITHUB_DISPATCH_TOKEN = "token";
  process.env.GITHUB_REPOSITORY = "VahramSargsyan/CryptoSignals";
  process.env.GITHUB_MONITOR_WORKFLOW = "relative-rotation-paper-live-v1.yml";

  let captured;
  global.fetch = async (url, options) => {
    captured = { url, options };
    return {
      status: 204,
      text: async () => ""
    };
  };

  try {
    await scheduler.dispatchMonitor("evening");
    assert.match(
      captured.url,
      /VahramSargsyan\/CryptoSignals\/actions\/workflows\/relative-rotation-paper-live-v1\.yml\/dispatches$/
    );
    assert.equal(captured.options.method, "POST");
    assert.equal(captured.options.headers.authorization, "Bearer token");
    assert.deepEqual(JSON.parse(captured.options.body), {
      ref: "main",
      inputs: { notification_mode: "evening" }
    });
  } finally {
    global.fetch = oldFetch;
    if (oldToken === undefined) delete process.env.GITHUB_DISPATCH_TOKEN;
    else process.env.GITHUB_DISPATCH_TOKEN = oldToken;
    if (oldRepo === undefined) delete process.env.GITHUB_REPOSITORY;
    else process.env.GITHUB_REPOSITORY = oldRepo;
    if (oldWorkflow === undefined) delete process.env.GITHUB_MONITOR_WORKFLOW;
    else process.env.GITHUB_MONITOR_WORKFLOW = oldWorkflow;
  }
});
