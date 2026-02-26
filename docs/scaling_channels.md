# Scaling workflow: add a new channel + enable YouTube uploads

Use this runbook when onboarding a new channel without changing code.

## 1) Create channel in DB via API

Create a channel with its slug and display name:

```bash
curl -X POST http://<VM_IP>:8080/v1/channels \
  -H 'Content-Type: application/json' \
  -d '{
    "slug": "new-channel-slug",
    "display_name": "New Channel Display Name"
  }'
```

Notes:
- `slug` is used in filesystem paths and token lookup.
- Keep slug stable after creation.

## 2) Place YouTube token for that slug

Store OAuth token file at:

```text
${YT_TOKENS_DIR}/${slug}/token.json
```

Example:

```text
YT_TOKENS_DIR=/secure/youtube/channels
slug=new-channel-slug
token=/secure/youtube/channels/new-channel-slug/token.json
```

## 3) Confirm uploader environment variables

Uploader requires:
- `YT_CLIENT_SECRET_JSON` (global YouTube OAuth client secret JSON)
- `UPLOAD_BACKEND=youtube`
- `YT_TOKENS_DIR` (base directory containing per-channel token folders)

Do not commit secrets or token files to git.

## 4) Sanity check end-to-end

1. Create a job in API for a Project that uses the new channel.
2. Let uploader pick up the job.
3. Check uploader logs and confirm it resolves the token path for the new slug (for example `${YT_TOKENS_DIR}/new-channel-slug/token.json`).

A minimal log check:

```bash
journalctl -u factory-uploader -n 200 --no-pager | rg 'token|new-channel-slug'
```

## Safety for testing

- During validation uploads, keep videos **Private** or **Unlisted**.
- Never paste secrets/tokens into docs, tickets, or chat.
