# Relative Rotation Telegram Control Bridge

Small stateless Vercel Function that receives Telegram webhook updates and turns an explicit user execution confirmation into a GitHub Actions `workflow_dispatch`.

## Security boundary

The bridge does **not** trade on an exchange.

It accepts only Telegram updates signed by Telegram's webhook secret header. An optional `TELEGRAM_ALLOWED_USER_ID` further restricts the actor.

A Telegram "done" button does not mutate GitHub immediately. It creates a ForceReply prompt. Only the user's numeric execution reply dispatches the GitHub control workflow.

GitHub re-validates:
- current book;
- current held asset;
- destination;
- latest strongest unresolved CONFIRMED signal;
- quantities.

## Required Vercel environment variables

- `TELEGRAM_BOT_TOKEN` — encrypted
- `TELEGRAM_WEBHOOK_SECRET` — encrypted
- `GITHUB_DISPATCH_TOKEN` — encrypted fine-grained GitHub token with **Actions: write** for this repository only
- `TELEGRAM_ALLOWED_USER_ID` — recommended; Telegram numeric user id

Optional:

- `GITHUB_REPOSITORY` — defaults to `VahramSargsyan/CryptoSignals`
- `GITHUB_CONTROL_WORKFLOW` — defaults to `relative-rotation-telegram-control-v1.yml`

## Vercel project

Import the GitHub repository and set the project root directory to:

`apps/rr-telegram-bridge`

## Telegram webhook

After production deploy, register:

`https://<project>.vercel.app/api/telegram`

as the bot webhook and use the same `TELEGRAM_WEBHOOK_SECRET` as Telegram's `secret_token`.

Do not put any secret in Git, workflow inputs, screenshots, or chat messages.
