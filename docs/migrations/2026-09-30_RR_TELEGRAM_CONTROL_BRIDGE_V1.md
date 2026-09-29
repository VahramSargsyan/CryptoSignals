# RR Telegram Control Bridge V1 - Migration Plan

Date: 2026-09-30
Workflow mode: BUILD_NEW_APP

## Change type

Cross-system integration and operational mutation path.

No strategy formula changes.
No exchange execution automation.
No change to the RR U10 universe.

## New components

- Vercel stateless Telegram webhook app
- GitHub workflow_dispatch control workflow
- validated manual execution apply script
- Telegram inline-button support behind a disabled-by-default feature gate
- Python + Node regression tests

## Data impact

Canonical files remain:
- config/relative_rotation_paper_live_v1.json
- research/relative_rotation/REAL_ROTATION_LOG.md

No new database is introduced.

The control workflow may change:
- position_books[n].held_asset
- position_books[n].quantity
- position_books[n].quantity_source
- top-level held_asset only when BOOK_1 changes

The real log receives an append-only execution record and its book summary is updated.

## Idempotency

Telegram update id is written into the real rotation log.

A repeated workflow dispatch with the same update id becomes a no-op.

## Security migration

New secret material exists only outside Git:
- Vercel TELEGRAM_BOT_TOKEN
- Vercel TELEGRAM_WEBHOOK_SECRET
- Vercel GITHUB_DISPATCH_TOKEN
- optional Vercel TELEGRAM_ALLOWED_USER_ID

The GitHub dispatch token must be repository-scoped and use minimum required Actions: write permission.

Existing GitHub Telegram bot/chat secrets remain unchanged.

## Feature activation

The code is safe to merge before deployment because outbound control buttons are disabled unless:

RR_TELEGRAM_CONTROL_ENABLED=true

Activation happens only after Vercel and Telegram webhook verification.

## Rollback

1. Set RR_TELEGRAM_CONTROL_ENABLED=false.
2. Remove Telegram webhook or point it away from Vercel.
3. Revoke GITHUB_DISPATCH_TOKEN.
4. Revert bridge/workflow files if full rollback is required.

No rollback of valid already-recorded manual trades should be performed automatically.

## Acceptance

- Python execution-control tests pass.
- Existing RR sender + monitor tests pass.
- Node bridge syntax/tests pass.
- PR RR live-data dry run passes without sending Telegram from PR.
- main merge leaves feature disabled by default.
- production promotion is reported separately after Vercel/webhook setup.

## Residual risks

- Vercel becomes an external ingress dependency.
- GitHub Actions remains an external execution dependency.
- A user may enter the wrong received quantity; GitHub can validate route/state but cannot independently verify an exchange fill without exchange data.
- Telegram confirmation time is not the exact exchange execution time.
- Fine-grained PAT rotation is an operational responsibility.
