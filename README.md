# 📦 OpenAlex Preprint Metadata Collector

A Streamlit web app to **search, select, and download metadata** for preprint servers from the [OpenAlex API](https://openalex.org/).  
It provides **flattened metadata**, **yearly publication trends**, and (optionally) **monthly trends** for any list of preprint servers.

---

## 🚀 Features

- **Search & Resolve:** Enter preprint server names or IDs and match them to OpenAlex sources.
- **Batch Processing:** Select multiple servers and fetch their metadata in one go.
- **Customizable:**  
  - Choose **primary location** / **host venue** filters.
  - Narrow by **date range** (for monthly trends).
  - **Monthly aggregation** toggle — OFF by default for faster runs.
- **Theming:** Light/Dark/Auto mode with custom accent color.
- **Progress Logs:** Live per-server progress panel with logs.
- **Downloadable ZIP** containing:
  - `servers.csv` – Flattened metadata with topics split into 3 columns.
  - `server_yearly_trends.csv` – Works count & citation count per year.
  - `server_monthly_trends.csv` – Monthly counts (if enabled).
  - Raw JSON for each server in a `json/` folder.

---

## 📂 Output CSVs

### 1. `servers.csv`
| source_id | display_name | type | homepage_url | topics_display | topics_subfields | topics_domains | ... |
|-----------|--------------|------|--------------|----------------|------------------|----------------|-----|
| `S12345`  | bioRxiv      | preprint | https://biorxiv.org | Genomics (345); Epidemiology (210) | Biology; Health Sciences | Life Sciences; Health |

### 2. `server_yearly_trends.csv`
| source_id | display_name | metric          | 2018 | 2019 | 2020 | 2021 |
|-----------|--------------|-----------------|------|------|------|------|
| S12345    | bioRxiv      | works_count     | 5000 | 6000 | 7200 | 8100 |
| S12345    | bioRxiv      | cited_by_count  | 1500 | 2400 | 3200 | 4100 |

### 3. `server_monthly_trends.csv` (optional)
| source_id | display_name | metric         | 2018-01 | 2018-02 | ... |
|-----------|--------------|----------------|---------|---------|-----|
| S12345    | bioRxiv      | works_count    | 450     | 500     | ... |
| S12345    | bioRxiv      | cited_by_count | 130     | 145     | ... |

---

## 📋 Requirements

- Python 3.9+
- See [`requirements.txt`](requirements.txt) for dependencies.

---

## 🖥️ Running Locally

```bash
# 1. Clone this repository
git clone https://github.com/your-username/openalex-preprint-metadata.git
cd openalex-preprint-metadata

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the Streamlit app
streamlit run streamlit_openalex_batch_app.py
