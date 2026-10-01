# Relative Rotation Telegram Control Bridge

Small stateless Vercel service used by Relative Rotation for two narrowly scoped duties:

1. receive signed Telegram execution confirmations and dispatch the GitHub control workflow;
2. provide a Vercel Cron scheduler that dispatches the existing GitHub paper-live monitor on the intended Yerevan notification windows.

## Security boundary

The bridge does **not** trade on an exchange.

Telegram control accepts only updates signed by Telegram's webhook secret header. An optional `TELEGRAM_ALLOWED_USER_ID` further restricts the actor.

The scheduler accepts only requests carrying Vercel's `Authorization: Bearer <CRON_SECRET>` header.

A Telegram "done" button does not mutate GitHub immediately. It creates a ForceReply prompt. Only the user's numeric execution reply dispatches the GitHub control workflow.

GitHub re-validates:
- current book;
- current held asset;
- destination;
- latest strongest unresolved CONFIRMED signal;
- quantities.

## Required Vercel environment variables

Scheduler:
- `GITHUB_DISPATCH_TOKEN` — encrypted fine-grained GitHub token with **Actions: write** for this repository only
- `CRON_SECRET` — long random secret used by Vercel Cron
- `GITHUB_REPOSITORY` — recommended; `VahramSargsyan/CryptoSignals`
- `GITHUB_MONITOR_WORKFLOW` — recommended; `relative-rotation-paper-live-v1.yml`

Telegram control, when the webhook is enabled:
- `TELEGRAM_BOT_TOKEN` — encrypted
- `TELEGRAM_WEBHOOK_SECRET` — encrypted
- `TELEGRAM_ALLOWED_USER_ID` — recommended; Telegram numeric user id
- `GITHUB_CONTROL_WORKFLOW` — recommended; `relative-rotation-telegram-control-v1.yml`

The operational workflow for missed-morning and position-sync commands defaults to
`relative-rotation-telegram-ops-v1.yml`; no extra environment variable is required.

## Vercel project

Import the GitHub repository and set the project root directory to:

`apps/rr-telegram-bridge`

The health endpoint is:

`https://<project>.vercel.app/api/health`

## Cron schedule

Vercel Cron is the primary scheduler. Existing GitHub `schedule` entries remain as a backup path and both routes share the GitHub-side Telegram dedupe state.

Schedules are UTC:

- `30 6 * * *` -> 10:30 Asia/Yerevan -> morning notification
- `30 18 * * *` -> 22:30 Asia/Yerevan -> evening notification

The evening stage is fail-closed. It is eligible only after Vahram explicitly marks the
morning signal as missed in Telegram for that same Yerevan date. GitHub then rebuilds
the current RR evidence before the 22:30 reminder can be sent.

The scheduler dispatches `relative-rotation-paper-live-v1.yml` with
`notification_mode=morning|evening`. It never performs an exchange order.

## Telegram operational commands

Supported examples:

- `/missed BOOK_2`
- `пропустил утренний сигнал BOOK_2`
- `пропустил сигнал` — allowed only when GitHub can resolve exactly one unresolved CONFIRMED book
- `/position BOOK_2 TRX 3950.7453`
- `позиция BOOK_1 ATOM 81,5`
- `ротация BOOK_2 FIL 777`

The morning CONFIRMED message also exposes an inline
`⏰ Пропустил утром BOOK_N` button. It arms only that BOOK's same-day evening stage.

`/position` and its Russian aliases synchronize the user-declared current asset and
quantity into the canonical BOOK state and real-rotation log. This operation never
places an exchange order and never invents missing CONFIRMED/fee/slippage/trade evidence.

## Telegram webhook

After production deploy, register:

`https://<project>.vercel.app/api/telegram`

as the bot webhook and use the same `TELEGRAM_WEBHOOK_SECRET` as Telegram's `secret_token`.

Do not put any secret in Git, workflow inputs, screenshots, or chat messages.
