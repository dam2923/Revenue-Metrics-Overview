# Kato — Sales & Marketing Board Dashboard

A static dashboard that visualises board-level sales & marketing metrics from HubSpot, with a one-click "Download as Excel" export.

- Pulls live HubSpot data via a scheduled GitHub Action (token stays in GitHub Secrets, never in the browser)
- Hosted on **Cloudflare Pages** (free, supports private GitHub repos)
- Pure HTML / CSS / Chart.js / SheetJS — no build step

## How the live data works

HubSpot's CRM API does not allow CORS calls from arbitrary browsers, so a static page can't call HubSpot directly. Instead:

1. An hourly **GitHub Action** runs `scripts/fetch.mjs` server-side, using a `HUBSPOT_TOKEN` from GitHub Secrets.
2. It writes a fresh `public/data.json` and commits it to the repo.
3. **Cloudflare Pages** is wired to the repo and auto-deploys whenever `main` changes.
4. The dashboard fetches `data.json` and renders.

Result: near-live data, secure token, free hosting, private repo.

## What's inside

```
hubspot-board-dashboard/
├── public/
│   ├── index.html          ← dashboard
│   ├── styles.css
│   ├── app.js              ← KPIs, charts, Excel export (SheetJS)
│   └── data.json           ← snapshot, refreshed by Action
├── scripts/
│   └── fetch.mjs           ← Node script that calls HubSpot
├── .github/workflows/
│   └── refresh.yml         ← hourly fetch + Pages deploy
├── package.json
├── .gitignore
└── README.md
```

## One-time setup

### 1. Create a HubSpot Private App + token

1. In HubSpot, go to **Settings → Integrations → Private Apps → Create private app**
2. Name it "Board Dashboard"
3. Under **Scopes**, enable read for: `crm.objects.deals.read`, `crm.objects.contacts.read`, `crm.objects.companies.read`, `crm.objects.owners.read`, `crm.schemas.deals.read`, `crm.schemas.contacts.read`, `crm.schemas.companies.read`
4. Click **Create app** → copy the **Access token** (starts `pat-eu1-…`)

### 2. Create the GitHub repo (private)

```bash
cd hubspot-board-dashboard
git init
git add .
git commit -m "Initial board dashboard"
gh repo create kato-board-dashboard --private --source=. --push
```

(or create via github.com → New repository → set **Private** → push)

### 3. Add the HubSpot token as a repo secret

GitHub repo → **Settings → Secrets and variables → Actions → New repository secret**
- **Name:** `HUBSPOT_TOKEN`
- **Value:** the `pat-eu1-…` token from step 1

### 4. Connect Cloudflare Pages to the repo

1. Create a free Cloudflare account at https://dash.cloudflare.com (no card required for Pages).
2. **Workers & Pages → Create → Pages → Connect to Git**
3. Authorise Cloudflare to access your **private** GitHub repo, then select `kato-board-dashboard`.
4. **Build settings:**
   - Framework preset: **None**
   - Build command: *(leave blank)*
   - Build output directory: `public`
   - Root directory: *(leave blank)*
5. Click **Save and Deploy**.

Cloudflare gives you a URL like `https://kato-board-dashboard.pages.dev/`. Every push to `main` (including the hourly data refresh from the GitHub Action) triggers an automatic redeploy in ~30 seconds.

Optional: add a custom domain in Cloudflare → Pages → Custom domains.

### 5. Run the data fetch workflow

GitHub repo → **Actions** tab → enable workflows if prompted → **"Refresh HubSpot data" → Run workflow**.

This fetches HubSpot data, commits `public/data.json`, and Cloudflare Pages will redeploy automatically. After the first manual run, the workflow runs hourly on its own.

## Local development

```bash
export HUBSPOT_TOKEN="pat-eu1-..."
npm run fetch          # writes public/data.json from live HubSpot
npm run serve          # serves dashboard at http://localhost:8080
```

## Customising metrics

- Add/remove KPI tiles in `public/index.html` and update `app.js#render()`
- Pipeline IDs are mapped at the top of `scripts/fetch.mjs` — adjust if you add pipelines
- Extra fields: add to the `props` array in `pullDeals()` and surface them in the rollups

## Hosting alternatives

If you'd rather not use Cloudflare:
- **Vercel** — free hobby tier, private repo OK. Set output dir to `public`.
- **Netlify** — free, similar to Cloudflare. Build command blank, publish dir `public`.
- **GitHub Pages** — works only if you upgrade to GitHub Pro/Team (private repo + Pages requires paid plan). Re-add a `deploy` job to the workflow.
- **Run locally** — `npm run serve` and screen-share for the board meeting.

## Security notes

- Token lives only in GitHub Secrets and the GH Action runner — never in client code
- Repo should be **private** (the dashboard contains your CRM numbers)
- Anyone with read access to the repo or the Pages URL can see the dashboard

## Refreshing on demand

Click **↻ Refresh** in the top-right of the dashboard to re-read `data.json`. To pull fresh numbers from HubSpot before refreshing the page, manually trigger the workflow from the Actions tab (or wait up to an hour).
