# Meta Ad Manager — Automation Workflow

Extract advertising data from the **Meta (Facebook) Marketing API**, download ad images to **Google Drive**, and log all performance metrics to **Google Sheets**.

---

## Architecture

```
Campaigns → Ad Sets → Ads → Creative (image) + Insights (metrics)
                                  ↓                    ↓
                          Download Image          Collect Metrics
                                  ↓
                        Upload to Google Drive
                                  ↓
                        Get Shareable Link
                                  ↓
                    Append Row to Google Sheets
```

## Prerequisites

- **Python 3.9+**
- A **Meta (Facebook) App** with Marketing API access
- A **Google Cloud** project with Drive API and Sheets API enabled
- A **Google Service Account** key file (JSON)

---

## Setup

### 1. Clone & install dependencies

```bash
cd "Meta ad manager"
pip install -r requirements.txt
```

### 2. Configure Meta API

1. Create a Meta App at [developers.facebook.com](https://developers.facebook.com).
2. Add the **Marketing API** product.
3. Generate a **User Access Token** with `ads_read` permission.
4. Note your **Ad Account ID** (format: `act_XXXXXXXXX`).

### 3. Configure Google APIs

1. Go to the [Google Cloud Console](https://console.cloud.google.com).
2. Enable **Google Drive API** and **Google Sheets API**.
3. Create a **Service Account** and download the JSON key file.
4. Place the key file at `credentials/service_account.json`.
5. Create a Google Sheet and a Drive folder for images.
6. **Share** both the Sheet and Drive folder with the service account email (found in the JSON, ending in `@*.iam.gserviceaccount.com`).

### 4. Set environment variables

Copy the template and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```ini
META_ACCESS_TOKEN=your_long_lived_token
META_AD_ACCOUNT_ID=act_123456789
META_API_VERSION=v21.0

GOOGLE_CREDENTIALS_FILE=credentials/service_account.json
GOOGLE_DRIVE_FOLDER_ID=your_drive_folder_id
GOOGLE_SHEET_ID=your_sheet_id
GOOGLE_SHEET_NAME=Ad Data
```

---

## Usage

### Full run

```bash
python main.py
```

### Dry run (no API calls)

```bash
python main.py --dry-run
```

---

## Output

### Google Sheet columns (17)

| # | Column |
|---|--------|
| 1 | Campaign Name |
| 2 | Campaign ID |
| 3 | Ad Set Name |
| 4 | Ad Set ID |
| 5 | Ad Name |
| 6 | Ad ID |
| 7 | Image URL (original) |
| 8 | Google Drive Image Link |
| 9 | Impressions |
| 10 | Reach |
| 11 | Clicks |
| 12 | CTR |
| 13 | CPC |
| 14 | Spend |
| 15 | Frequency |
| 16 | Conversions |
| 17 | Date Extracted |

---

## Project Structure

```
Meta ad manager/
├── config.py              # Configuration (env vars)
├── meta_api.py            # Meta Marketing API client
├── image_downloader.py    # Download images from URLs
├── google_drive.py        # Google Drive upload + sharing
├── google_sheets.py       # Google Sheets row appender
├── main.py                # Orchestrator
├── requirements.txt       # Dependencies
├── .env.example           # Environment variable template
├── .gitignore
├── credentials/           # Service account key (gitignored)
└── downloads/             # Temp image storage (gitignored)
```

---

## Error Handling

- **Rate limits** — Meta API calls retry with exponential backoff on HTTP 429.
- **Per-ad isolation** — If one ad fails, the workflow continues with the rest.
- **Batch fallback** — If batch Sheets write fails, falls back to single-row appends.
- **Missing images** — Ads without images are still logged (image columns left empty).

---

## License

Private project — not for redistribution.
