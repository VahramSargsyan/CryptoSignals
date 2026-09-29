# Relative Rotation Telegram Control Bridge V1

Status: BUILD_NEW_APP / NOT YET ENABLED IN PRODUCTION
Date: 2026-09-30

## Objective

Allow Vahram to acknowledge a manually executed Relative Rotation swap from Telegram and have GitHub validate and record the execution without any exchange API trading.

Flow:

Telegram signal
-> explicit "Done" button
-> Telegram ForceReply quantity confirmation
-> Vercel webhook
-> GitHub workflow_dispatch
-> rebuild current RR evidence
-> validate latest strongest unresolved CONFIRMED route
-> update canonical book state + REAL_ROTATION_LOG
-> tests
-> commit to main
-> Telegram acknowledgement

## Safety boundary

This bridge never places an exchange order.

A Telegram click alone does not mutate state. The user must also reply with actual execution quantities.

GitHub is the final authority before mutation. It checks:
- current book id;
- current held asset;
- TARGET destination;
- current configured quantity when known;
- latest strongest unresolved CONFIRMED event for that book;
- signal date;
- idempotency via Telegram update id.

## Source of truth

Canonical position state remains:

`config/relative_rotation_paper_live_v1.json -> position_books`

Canonical manual audit log remains:

`research/relative_rotation/REAL_ROTATION_LOG.md`

The Vercel function is stateless and is not a trading ledger.

## Telegram interaction

Buttons are only emitted when GitHub repository variable:

`RR_TELEGRAM_CONTROL_ENABLED=true`

Otherwise existing Telegram alerts remain text-only.

For a CONFIRMED route the enabled bot adds:
- Done <BOOK>
- Later <BOOK>

Done creates a ForceReply prompt.

If the book quantity is already known, the user may answer with only the received quantity.

If source quantity is unknown, the user must answer:

`SENT RECEIVED`

The Telegram message timestamp is stored as the confirmation timestamp. It is not claimed to be the exact exchange fill time.

## Vercel bridge

Project root:

`apps/rr-telegram-bridge`

Required encrypted environment variables:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_WEBHOOK_SECRET
- GITHUB_DISPATCH_TOKEN

Recommended:
- TELEGRAM_ALLOWED_USER_ID

The GitHub token should be a fine-grained token restricted to this repository with Actions: write only.

Optional:
- GITHUB_REPOSITORY
- GITHUB_CONTROL_WORKFLOW

## GitHub control workflow

Workflow:

`.github/workflows/relative-rotation-telegram-control-v1.yml`

The workflow:
1. checks out main;
2. runs control + RR regression tests;
3. rebuilds live RR evidence from public market data;
4. validates the requested execution;
5. mutates only the RR position config and real rotation log;
6. refuses to commit if any unexpected file changed;
7. refuses stale mutation if main advanced during validation;
8. commits canonical state;
9. sends Telegram success/failure acknowledgement.

## Failure behavior

If Vercel cannot dispatch GitHub:
- no canonical position change occurs;
- Telegram receives an error.

If GitHub validation fails:
- no commit is made;
- Telegram receives a failure acknowledgement.

If the GitHub commit succeeds but acknowledgement delivery fails:
- canonical state is still correct;
- next RR monitor run reads the updated book and stops the old execution reminders.

## Rollback

Disable the feature immediately by removing or setting:

`RR_TELEGRAM_CONTROL_ENABLED=false`

Telegram returns to text-only alerts.

Then, if needed:
- remove the Telegram webhook;
- disable/delete the Vercel project;
- revoke GITHUB_DISPATCH_TOKEN;
- revert the bridge/control workflow code.

Existing RR manual state remains valid because the bridge writes the same canonical files used today.

## Promotion gate

Do not set RR_TELEGRAM_CONTROL_ENABLED=true until:
- branch tests pass;
- Vercel production deployment exists;
- /api/health returns OK;
- Vercel secrets are configured;
- Telegram webhook is registered with secret_token;
- one non-mutating callback test succeeds;
- GitHub control workflow is visible on main.
