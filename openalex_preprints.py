# streamlit_openalex_batch_app.py
"""
OpenAlex Preprint Servers — Batch Trends Builder (Guided UI)
=================================================================
What this app does
------------------
1) Lets you provide preprint server names (CSV upload or paste).
2) Resolves each name to one or more OpenAlex "Sources".
3) You choose the correct matches (or select all).
4) Fetches server metadata and builds:
   - servers.csv (flattened metadata, with 3 topic columns)
   - server_yearly_trends.csv (years as columns; rows are metrics)
   - server_monthly_trends.csv (optional and slower; placeholder if disabled)
   - json/ folder with raw source JSON (inside a ZIP you can download)

Key options
-----------
- Monthly aggregation (slow): toggle ON only if you need monthly trends.
- Date range (YYYY-MM-DD): narrows monthly aggregation queries.
- Theme: Light/Dark/Auto and an accent color – for a friendly, branded feel.
- Per-server progress panel: see live logs & progress while building.

Notes
-----
- Add a valid email to use the OpenAlex "polite pool" (recommended).
- Monthly trends require scanning Works; it's the slowest part. Keep it OFF
  for quick runs or test passes.
"""

import io
import json
import time
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st

# ──────────────────────────────────────────────────────────────────────────────
# App config & constants
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OpenAlex Preprint Servers — Batch Trends",
    page_icon="📦",
    layout="wide",
)
OPENALEX_BASE = "https://api.openalex.org"

# ──────────────────────────────────────────────────────────────────────────────
# THEME: runtime Light/Dark/Auto + Accent color
# ──────────────────────────────────────────────────────────────────────────────
def apply_runtime_theme(mode: str, accent: str):
    """Apply a simple runtime theme using CSS variables."""
    is_dark = (mode == "Dark") or (mode == "Auto" and st.get_option("theme.base") == "dark")
    bg = "#0E1117" if is_dark else "#FFFFFF"
    text = "#FAFAFA" if is_dark else "#111111"
    subtle = "#161b22" if is_dark else "#f6f8fa"

    st.markdown(
        f"""
        <style>
        :root {{
          --acc: {accent};
          --bg: {bg};
          --text: {text};
          --subtle: {subtle};
        }}
        .stApp {{ background: var(--bg); color: var(--text); }}
        .stButton>button, .stDownloadButton>button {{
          border-radius: 10px; border: 1px solid var(--acc); color: var(--text); background: transparent;
        }}
        .stButton>button:hover, .stDownloadButton>button:hover {{
          background: var(--acc); color: #fff;
        }}
        .stProgress > div > div > div > div {{ background-color: var(--acc) !important; }}
        .stExpander > div > div {{ border-bottom: 1px solid var(--acc); }}
        .stMetric label, .stMetric small {{ color: var(--text) !important; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Small utilities (HTTP + text)
# ──────────────────────────────────────────────────────────────────────────────
def api_get(url: str, sleep_s: float, max_retries: int = 5, mailto: Optional[str] = None) -> requests.Response:
    """GET with polite retry/backoff. Adds mailto param (recommended by OpenAlex)."""
    headers = {"User-Agent": "OpenAlexStreamlitBatch/1.1"}
    if mailto:
        url += ("&" if "?" in url else "?") + f"mailto={quote(mailto)}"
    backoff = sleep_s
    for _ in range(max_retries):
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code == 200:
            if sleep_s > 0:
                time.sleep(sleep_s)  # be polite to the API
            return r
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff *= 1.6
            continue
        r.raise_for_status()
    r.raise_for_status()
    return r

def norm_name(s: str) -> str:
    """Normalize whitespace for consistent matching."""
    return " ".join((s or "").strip().split())

# ──────────────────────────────────────────────────────────────────────────────
# OpenAlex: resolving & fetching
# ──────────────────────────────────────────────────────────────────────────────
def resolve_candidates(name: str, per_page: int, sleep_s: float, mailto: Optional[str]) -> List[Dict[str, Any]]:
    """Resolve a human-entered server name to OpenAlex Source candidates."""
    q = quote(name)
    url = f"{OPENALEX_BASE}/sources?filter=display_name.search:%22{q}%22&per-page={per_page}"
    r = api_get(url, sleep_s=sleep_s, mailto=mailto)
    results = r.json().get("results", [])
    if not results:
        url = f"{OPENALEX_BASE}/sources?search={q}&per-page={per_page}"
        r = api_get(url, sleep_s=sleep_s, mailto=mailto)
        results = r.json().get("results", [])
    for c in results:
        c["short_id"] = (c.get("id") or "").replace("https://openalex.org/", "")
    return results

def fetch_source(sid: str, sleep_s: float, mailto: Optional[str]) -> Dict[str, Any]:
    """Fetch a full Source record from OpenAlex."""
    url = f"{OPENALEX_BASE}/sources/{sid}"
    return api_get(url, sleep_s=sleep_s, mailto=mailto).json()

def iter_works_for_source(
    sid: str,
    date_from: Optional[str],
    date_to: Optional[str],
    sleep_s: float,
    mailto: Optional[str],
    select_fields: str = "publication_date,cited_by_count",
    use_primary_location: bool = True,
    use_host_venue: bool = False,
):
    """Iterate works for a given Source. Used only for monthly aggregation."""
    filters = []
    if use_primary_location:
        filters.append(f"primary_location.source.id:{sid}")
    if use_host_venue:
        filters.append(f"host_venue.id:{sid}")
    if date_from:
        filters.append(f"from_publication_date:{date_from}")
    if date_to:
        filters.append(f"to_publication_date:{date_to}")
    filter_str = ",".join(filters)

    base_url = f"{OPENALEX_BASE}/works?per-page=200&cursor=*"
    if filter_str:
        base_url += f"&filter={quote(filter_str)}"
    if select_fields:
        base_url += f"&select={quote(select_fields)}"

    next_url = base_url
    while True:
        r = api_get(next_url, sleep_s=sleep_s, mailto=mailto)
        data = r.json()
        for w in data.get("results", []):
            yield {"publication_date": w.get("publication_date"), "cited_by_count": w.get("cited_by_count", 0)}
        nxt = data.get("meta", {}).get("next_cursor")
        if not nxt:
            break
        next_url = base_url.replace("cursor=*", f"cursor={quote(nxt)}")

# ──────────────────────────────────────────────────────────────────────────────
# Flattening + topic columns
# ──────────────────────────────────────────────────────────────────────────────
def _stringify(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (str, int, float, bool)):
        return str(x)
    return json.dumps(x, ensure_ascii=False)

def flatten_json(obj: Any, prefix: str = "", out: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Flatten nested JSON into a 1-level dict of string columns."""
    if out is None:
        out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}__{k}" if prefix else k
            if isinstance(v, dict):
                flatten_json(v, key, out)
            elif isinstance(v, list):
                if all(isinstance(el, dict) for el in v):
                    for i, el in enumerate(v):
                        flatten_json(el, f"{key}_{i}", out)
                else:
                    out[key] = "|".join(_stringify(el) for el in v)
            else:
                out[key] = _stringify(v)
    elif isinstance(obj, list):
        if all(isinstance(el, dict) for el in obj):
            for i, el in enumerate(obj):
                flatten_json(el, f"{prefix}_{i}" if prefix else str(i), out)
        else:
            out[prefix or "value"] = "|".join(_stringify(el) for el in obj)
    else:
        out[prefix or "value"] = _stringify(obj)
    return out

def build_topics_columns(topics: Any):
    """
    Convert OpenAlex 'topics' list into three friendly columns:
      - topics_display: "Topic (count); Topic2 (count2); ..."
      - topics_subfields: "Subfield; Subfield2; ..."
      - topics_domains: "Domain; Domain2; ..."
    """
    if not isinstance(topics, list):
        return "", "", ""
    disp_list, sub_list, dom_list = [], [], []
    for t in topics:
        if not isinstance(t, dict):
            continue
        t_name = t.get("display_name", "")
        t_count = t.get("count", "")
        if t_name:
            disp_list.append(f"{t_name} ({t_count})" if t_count != "" else t_name)
        sf_name = (t.get("subfield") or {}).get("display_name", "")
        if sf_name:
            sub_list.append(sf_name)
        dom_name = (t.get("domain") or {}).get("display_name", "")
        if dom_name:
            dom_list.append(dom_name)
    return "; ".join(disp_list), "; ".join(sub_list), "; ".join(dom_list)

def iso_to_year_month(iso_date: str) -> Optional[str]:
    """Normalize dates to YYYY-MM for monthly bins."""
    if not iso_date:
        return None
    try:
        dt = datetime.fromisoformat(iso_date)
        return f"{dt.year:04d}-{dt.month:02d}"
    except Exception:
        parts = str(iso_date).split("-")
        if len(parts) == 2:
            y, m = parts
            return f"{int(y):04d}-{int(m):02d}"
        if len(parts) == 1 and parts[0].isdigit():
            return f"{int(parts[0]):04d}-01"
    return None

# ──────────────────────────────────────────────────────────────────────────────
# HEADER: Overview + Quick Start
# ──────────────────────────────────────────────────────────────────────────────
st.title("🗂️OpenAlex Preprint Servers Metadata Collector")
st.caption("Collect metadata + yearly trends (and optional monthly trends) for preprint servers from OpenAlex.")

with st.expander("👋 Quick Start (click to open)", expanded=True):
    st.markdown(
        """
**Step 1.** Provide your server names:
- Upload a CSV (we use the first column), or
- Paste names (one per line) in the text box.

**Step 2.** Click **“Resolve server names…”** to find OpenAlex matches.

**Step 3.** In *Review matches*, select the correct Sources for each name (or **Select ALL**).

**Step 4.** Choose options in the sidebar:
- **Monthly aggregation** is OFF by default (faster).
- Add **From/To dates** if you use monthly aggregation.

**Step 5.** Click **“Fetch & Build ZIP”**.
- Watch live logs and per-server progress.
- Download the ZIP with CSVs and raw JSON.

> Tip: For quick tests, keep monthly OFF; you’ll still get yearly trends.
        """
    )

# Sidebar: THEME & OPTIONS
with st.sidebar:
    st.header("🎨 Theme")
    theme_mode = st.radio("Mode", options=["Dark", "Auto", "Light"], index=0, horizontal=True,
                          help="Auto follows your Streamlit/OS theme.")
    accent_color = st.color_picker("Accent color", value="#6C63FF", help="Pick a brand or favorite color.")
    apply_runtime_theme(theme_mode, accent_color)

with st.sidebar:
    st.header("⚙️ Options")
    mailto = st.text_input(
        "Polite pool email (recommended)",
        value="",
        help="OpenAlex recommends a contact email to prioritize your requests politely."
    )
    date_from = st.text_input(
        "From publication date (YYYY-MM-DD) — optional",
        value="",
        help="Only used when monthly aggregation is enabled. Example: 2015-01-01"
    )
    date_to = st.text_input(
        "To publication date (YYYY-MM-DD) — optional",
        value="",
        help="Only used when monthly aggregation is enabled. Example: 2025-12-31"
    )
    per_page = st.number_input(
        "Max candidates per server",
        min_value=1, max_value=100, value=25, step=1,
        help="How many OpenAlex search matches to show per input name."
    )
    sleep_s = st.number_input(
        "Sleep between API calls (seconds)",
        min_value=0.0, max_value=3.0, value=0.6, step=0.1,
        help="Polite delay between requests; increase if you hit rate limits."
    )
    use_primary_location = st.checkbox(
        "Filter by primary_location.source.id",
        value=True,
        help="Recommended. Counts works where the preprint server is the primary location."
    )
    use_host_venue = st.checkbox(
        "Also filter by host_venue.id",
        value=False,
        help="Broader filter – includes works where the server is a host venue."
    )
    monthly_enabled = st.checkbox(
        "Include monthly aggregation (slower)",
        value=False,
        help="When ON, we iterate Works to build monthly counts. This can take time."
    )
    st.markdown("---")
    st.markdown("**Input methods**")
    uploaded_csv = st.file_uploader(
        "Upload CSV of server names (first column used)",
        type=["csv"],
        help="CSV with one server name per row; we read only the first column."
    )
    manual_input = st.text_area(
        "Or paste/type names (one per line)",
        help="Example: bioRxiv\nmedRxiv\narXiv"
    )

    # Handy template download to get people started quickly
    template_csv = "server_name\nbioRxiv\nmedRxiv\narXiv\n"
    st.download_button(
        "Download CSV template",
        data=template_csv.encode("utf-8"),
        file_name="preprint_servers_template.csv",
        mime="text/csv",
        help="A starter CSV: edit and upload back here."
    )

# ──────────────────────────────────────────────────────────────────────────────
# STEP 1: Parse names
# ──────────────────────────────────────────────────────────────────────────────
names: List[str] = []
if uploaded_csv is not None:
    try:
        df = pd.read_csv(uploaded_csv)
        if df.empty:
            st.warning("Uploaded CSV appears to be empty.")
        else:
            first_col = df.columns[0]
            names = [norm_name(x) for x in df[first_col].astype(str).tolist() if norm_name(x)]
    except Exception as e:
        st.error(f"Failed to read CSV: {e}")

if manual_input.strip():
    names.extend([norm_name(x) for x in manual_input.splitlines() if norm_name(x)])

# Deduplicate while preserving order
seen = set()
unique_names = []
for n in names:
    if n not in seen:
        seen.add(n)
        unique_names.append(n)

st.subheader("1) Server names detected")
if unique_names:
    st.success(f"Found **{len(unique_names)}** name(s).")
    st.code("\n".join(unique_names[:50]) + ("\n..." if len(unique_names) > 50 else ""), language=None)
else:
    st.info("Upload a CSV or paste/type server names to begin.")

# ──────────────────────────────────────────────────────────────────────────────
# STEP 2: Resolve candidates
# ──────────────────────────────────────────────────────────────────────────────
if "candidates_map" not in st.session_state:
    st.session_state.candidates_map = {}     # name -> [candidate dicts]
if "selections_map" not in st.session_state:
    st.session_state.selections_map = {}     # name -> [selected short_ids]
if "log_lines" not in st.session_state:
    st.session_state.log_lines = []

def log_app(msg: str, box: st.delta_generator.DeltaGenerator):
    """Append a timestamped line to the global log box."""
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.log_lines.append(f"[{ts}] {msg}")
    st.session_state.log_lines = st.session_state.log_lines[-400:]
    box.code("\n".join(st.session_state.log_lines), language=None)

st.markdown("### 2) Resolve your names to OpenAlex Sources")
st.write(
    "Click the button below to look up each name in OpenAlex and retrieve possible matches. "
    "You’ll then choose the correct one(s) for each input name."
)

if st.button("🔍 Resolve server names to OpenAlex Sources", key="btn_resolve", disabled=not unique_names):
    st.session_state.candidates_map = {}
    st.session_state.selections_map = {}
    st.session_state.log_lines = []
    prog = st.progress(0)
    log_box = st.empty()
    for idx, name in enumerate(unique_names, start=1):
        try:
            cands = resolve_candidates(name, per_page=per_page, sleep_s=sleep_s, mailto=mailto or None)
            log_app(f"Resolved '{name}' → {len(cands)} candidate(s).", log_box)
        except Exception as e:
            st.warning(f"Resolution failed for '{name}': {e}")
            log_app(f"Resolution failed for '{name}': {e}", log_box)
            cands = []
        st.session_state.candidates_map[name] = cands
        st.session_state.selections_map[name] = []  # clear selections
        prog.progress(idx / len(unique_names))
    st.success("Resolution complete. Review & select below.")

# -----------------------------
# Show candidate selectors (SAFE "Select all" with rerun)
# -----------------------------
if "labels_by_name" not in st.session_state:
    st.session_state.labels_by_name = {}   # name -> list[str] (human labels)
if "_select_all_flag" not in st.session_state:
    st.session_state["_select_all_flag"] = False
if "_select_all_one" not in st.session_state:
    st.session_state["_select_all_one"] = None

def prefill_selections_before_render():
    """
    If a select-all flag is set, prefill st.session_state for multiselect widgets
    BEFORE they are instantiated. Then clear the flag so normal interaction resumes.
    """
    # Global "select all"
    if st.session_state.get("_select_all_flag"):
        for nm, labels in st.session_state.labels_by_name.items():
            if labels:
                st.session_state[f"sel_{nm}"] = labels[:]  # copy
        st.session_state["_select_all_flag"] = False  # reset the flag

    # Per-name "select all"
    if st.session_state.get("_select_all_one"):
        nm = st.session_state["_select_all_one"]
        labels = st.session_state.labels_by_name.get(nm, [])
        if labels:
            st.session_state[f"sel_{nm}"] = labels[:]
        st.session_state["_select_all_one"] = None  # reset per-name flag

if st.session_state.candidates_map:
    st.subheader("2) Review matches and select")
    st.write(
        "For each of your input names, pick the correct OpenAlex Source(s). "
        "You may select multiple if that reflects your intent (e.g., sub-servers)."
    )

    # Build labels_by_name for this run (used both for rendering and select-all)
    st.session_state.labels_by_name = {}
    for name in unique_names:
        cands = st.session_state.candidates_map.get(name, [])
        labels = []
        for c in cands:
            label = (
                f"{c.get('display_name','(no name)')} "
                f"({c.get('type','?')}) — {c.get('short_id','?')} — "
                f"works:{c.get('works_count',0)} — {c.get('homepage_url','') or ''}"
            )
            labels.append(label)
        st.session_state.labels_by_name[name] = labels

    # IMPORTANT: prefill session_state for widgets BEFORE we render them
    prefill_selections_before_render()

    with st.expander("Open candidate lists", expanded=True):
        sel_all_clicked = st.button("Select ALL candidates for ALL names", key="select_all_global")
        if sel_all_clicked:
            st.session_state["_select_all_flag"] = True
            st.rerun()

        for name in unique_names:
            cands = st.session_state.candidates_map.get(name, [])
            if not cands:
                st.warning(f"No candidates found for: {name}")
                continue

            labels = st.session_state.labels_by_name.get(name, [])
            value_map = {
                (
                    f"{c.get('display_name','(no name)')} "
                    f"({c.get('type','?')}) — {c.get('short_id','?')} — "
                    f"works:{c.get('works_count',0)} — {c.get('homepage_url','') or ''}"
                ): c.get("short_id","")
                for c in cands
            }

            cols = st.columns([4,1])
            with cols[0]:
                current_default = st.session_state.get(f"sel_{name}", [])
                selected_labels = st.multiselect(
                    f"**{name}** — select your match(es)",
                    options=labels,
                    default=current_default,
                    key=f"sel_{name}",
                    help="Pick one or more Sources that correspond to your input name."
                )
            with cols[1]:
                per_name_clicked = st.button("Select all", key=f"selectall_btn_{name}")
                if per_name_clicked:
                    st.session_state["_select_all_one"] = name
                    st.rerun()

            st.session_state.selections_map[name] = [value_map[l] for l in selected_labels if l in value_map]

# ──────────────────────────────────────────────────────────────────────────────
# STEP 3: Build — per-server panels + logs + metrics + compact UI
# ──────────────────────────────────────────────────────────────────────────────
def build_zip_from_selection(
    selections_map: Dict[str, List[str]],
    sleep_s: float,
    mailto: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    use_primary_location: bool,
    use_host_venue: bool,
    monthly_enabled: bool,
    overall_log: st.delta_generator.DeltaGenerator,
    metrics: Dict[str, st.delta_generator.DeltaGenerator],
    # Compact UI elements
    compact_progress: Optional[st.delta_generator.DeltaGenerator] = None,
    compact_status: Optional[st.delta_generator.DeltaGenerator] = None,
    compact_log: Optional[st.delta_generator.DeltaGenerator] = None,
    compact_log_keep: int = 5,
    # NEW: toggle whether to render heavy progress details
    show_progress_details: bool = False,
) -> bytes:
    """Core builder: fetch sources, assemble CSVs, and produce ZIP bytes."""
    # Combine and dedupe all selected source_ids
    chosen_sids: List[str] = []
    for lst in selections_map.values():
        chosen_sids.extend(lst)
    seen = set()
    chosen_sids = [x for x in chosen_sids if x and not (x in seen or seen.add(x))]
    if not chosen_sids:
        raise ValueError("No sources selected.")

    total = len(chosen_sids)
    start_t = time.time()
    per_server_times: List[float] = []
    done = 0

    # Data accumulators
    servers_rows: List[Dict[str, str]] = []
    all_columns: Set[str] = set()
    yearly_data: Dict[str, Dict[str, Dict[str, int]]] = {}
    monthly_data: Dict[str, Dict[str, Dict[str, int]]] = {}
    sid_to_name: Dict[str, str] = {}
    years_seen: Set[str] = set()
    months_seen: Set[str] = set()

    # Optional heavy UI (metrics row + per-server panels)
    if show_progress_details:
        st.subheader("3) Build progress")
        grid_cols = st.columns(3)
        metrics["count"] = grid_cols[0].metric("Servers processed", f"0/{total}")
        metrics["avg"]  = grid_cols[1].metric("Average time", "–")
        metrics["eta"]  = grid_cols[2].metric("ETA (approx)", "–")

        panels = {}
        for sid in chosen_sids:
            panels[sid] = {
                "exp": st.expander(f"Server: {sid}", expanded=False),
                "progress": None,
                "log": None,
                "hist": [],
                "title": sid
            }
        overall_prog = st.progress(0)
    else:
        grid_cols = [st.empty(), st.empty(), st.empty()]
        panels = {}
        overall_prog = st.empty()

    # Global log helper (always feeds the compact view; full log only if shown)
    def log_overall(msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        st.session_state.log_lines.append(f"[{ts}] {msg}")
        st.session_state.log_lines = st.session_state.log_lines[-400:]
        # Only render into the big log box if visible
        if show_progress_details:
            overall_log.code("\n".join(st.session_state.log_lines), language=None)

    # Per-server log helper (no-op when panels are hidden)
    def log_server(sid: str, msg: str):
        if not show_progress_details:
            return
        ts = datetime.now().strftime("%H:%M:%S")
        block = panels[sid]
        if block["log"] is None:
            with block["exp"]:
                block["progress"] = st.progress(0)
                block["log"] = st.empty()
        block["hist"].append(f"[{ts}] {msg}")
        block["hist"] = block["hist"][-200:]
        block["log"].code("\n".join(block["hist"]), language=None)

    # Metrics helper
    def update_metrics():
        if not show_progress_details:
            return
        elapsed = time.time() - start_t
        avg = (sum(per_server_times) / len(per_server_times)) if per_server_times else 0.0
        remaining = max(0.0, (total - done) * avg)
        rem = int(remaining)
        h, rem2 = divmod(rem, 3600)
        m, s = divmod(rem2, 60)
        eta_str = (f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s") if remaining else "–"
        grid_cols[0].metric("Servers processed", f"{done}/{total}")
        grid_cols[1].metric("Average time", f"{avg:.1f}s/server" if avg else "–")
        grid_cols[2].metric("ETA (approx)", eta_str)

    # Compact UI helpers
    compact_lines: List[str] = []
    def push_compact(line: str):
        if compact_log is None:
            return
        compact_lines.append(line)
        if len(compact_lines) > compact_log_keep:
            compact_lines[:] = compact_lines[-compact_log_keep:]
        compact_log.text("\n".join(compact_lines))

    if compact_status is not None:
        compact_status.text(f"Servers processed: 0/{total}")
    if compact_progress is not None:
        compact_progress.progress(0)

    # Main fetch/build loop
    for sid in chosen_sids:
        t0 = time.time()

        # Compact heartbeat
        push_compact(f"{done+1}/{total} — Fetching {sid}…")

        log_overall(f"Fetching source {sid}")
        log_server(sid, "Fetching source JSON…")
        src = fetch_source(sid, sleep_s=sleep_s, mailto=mailto)
        display_name = src.get("display_name", "")

        if show_progress_details:
            # Update expander header with display name for clarity
            panels[sid]["title"] = display_name or sid
            panels[sid]["exp"].markdown(
                f"**Server:** {display_name or '(unknown)'}  \n"
                f"**OpenAlex ID:** `{sid}`"
            )

        # Prepare for CSV: remove heavy lists, convert topics to 3 columns, flatten
        src_clean = dict(src)
        for k in ("counts_by_year", "topic_share", "x_concepts"):
            src_clean.pop(k, None)
        topics_display, topics_subfields, topics_domains = build_topics_columns(src_clean.get("topics"))
        src_clean.pop("topics", None)
        flat = flatten_json(src_clean)
        flat["source_id"] = (src.get("id") or "").replace("https://openalex.org/", "")
        flat["display_name"] = display_name
        flat["topics_display"] = topics_display
        flat["topics_subfields"] = topics_subfields
        flat["topics_domains"] = topics_domains
        flat["raw_json"] = json.dumps(src, ensure_ascii=False)

        servers_rows.append(flat)
        all_columns.update(flat.keys())
        sid_to_name[flat["source_id"]] = display_name

        # Yearly trends (fast)
        cby = src.get("counts_by_year", []) or []
        for row in cby:
            y = str(row.get("year", ""))
            if y.isdigit():
                years_seen.add(y)
                yearly_data.setdefault(flat["source_id"], {}).setdefault(y, {"works_count": 0, "cited_by_count": 0})
                yearly_data[flat["source_id"]][y]["works_count"] = int(row.get("works_count", 0) or 0)
                yearly_data[flat["source_id"]][y]["cited_by_count"] = int(row.get("cited_by_count", 0) or 0)

        # Monthly trends (optional & slow)
        if monthly_enabled:
            log_server(sid, "Monthly aggregation started… (this can take a while)")
            works_processed = 0
            for w in iter_works_for_source(
                flat["source_id"], date_from or None, date_to or None,
                sleep_s=sleep_s, mailto=mailto,
                select_fields="publication_date,cited_by_count",
                use_primary_location=use_primary_location, use_host_venue=use_host_venue
            ):
                ym = iso_to_year_month(w.get("publication_date"))
                if not ym:
                    continue
                months_seen.add(ym)
                monthly_data.setdefault(flat["source_id"], {}).setdefault(ym, {"works_count": 0, "cited_by_count": 0})
                monthly_data[flat["source_id"]][ym]["works_count"] += 1
                monthly_data[flat["source_id"]][ym]["cited_by_count"] += int(w.get("cited_by_count", 0) or 0)
                works_processed += 1
                if works_processed % 500 == 0 and show_progress_details:
                    log_server(sid, f"…processed {works_processed} works so far")
                    if panels.get(sid, {}).get("progress") is not None:
                        pct = min(99, (works_processed // 5) % 100) / 100.0
                        panels[sid]["progress"].progress(pct)
            if show_progress_details:
                log_server(sid, f"Monthly aggregation complete. Total works scanned: {works_processed}")
                if panels.get(sid, {}).get("progress") is not None:
                    panels[sid]["progress"].progress(1.0)
        else:
            log_server(sid, "Monthly aggregation skipped (disabled).")

        # Per-server timing + UI refresh
        elapsed = time.time() - t0
        per_server_times.append(elapsed)
        done += 1

        # Compact updates
        push_compact(f"{done}/{total} — Done {display_name or sid} in {int(elapsed)}s")
        if compact_status is not None:
            compact_status.text(f"Servers processed: {done}/{total}")
        if compact_progress is not None:
            compact_progress.progress(done / total)

        # Existing overall progress + metrics
        if show_progress_details:
            overall_prog.progress(done / total)
        log_overall(f"Finished {display_name or sid} in {int(elapsed)}s")
        update_metrics()

    # ── Build the three CSVs as DataFrames ────────────────────────────────────
    preferred_first = [
        "source_id","display_name","type","homepage_url","issn_l","issn","country_code",
        "host_organization_name","host_organization","host_organization_lineage",
        "is_oa","is_in_doaj","is_indexed_in_scopus","is_core",
        "works_count","cited_by_count",
        "summary_stats__2yr_mean_citedness","summary_stats__h_index","summary_stats__i10_index",
        "ids__openalex","ids__wikidata",
        "topics_display","topics_subfields","topics_domains",
        "works_api_url","updated_date","created_date"
    ]
    remaining = sorted([c for c in all_columns if c not in preferred_first and c != "raw_json"])
    servers_header = [c for c in preferred_first if c in all_columns] + remaining + ["raw_json"]
    servers_df = pd.DataFrame([{col: row.get(col, "") for col in servers_header} for row in servers_rows])

    # Yearly wide
    years_sorted = sorted(years_seen, key=lambda x: int(x)) if years_seen else []
    yearly_rows = []
    for sid, year_map in yearly_data.items():
        disp = sid_to_name.get(sid, "")
        row_wc = {"source_id": sid, "display_name": disp, "metric": "works_count"}
        row_cb = {"source_id": sid, "display_name": disp, "metric": "cited_by_count"}
        for y in years_sorted:
            row_wc[y] = year_map.get(y, {}).get("works_count", 0)
            row_cb[y] = year_map.get(y, {}).get("cited_by_count", 0)
        yearly_rows.append(row_wc); yearly_rows.append(row_cb)
    yearly_df = pd.DataFrame(yearly_rows, columns=["source_id","display_name","metric"] + years_sorted)

    # Monthly wide (or placeholder)
    if monthly_enabled and months_seen:
        months_sorted = sorted(months_seen)
        monthly_rows = []
        for sid, month_map in monthly_data.items():
            disp = sid_to_name.get(sid, "")
            row_wc = {"source_id": sid, "display_name": disp, "metric": "works_count"}
            row_cb = {"source_id": sid, "display_name": disp, "metric": "cited_by_count"}
            for m in months_sorted:
                row_wc[m] = month_map.get(m, {}).get("works_count", 0)
                row_cb[m] = month_map.get(m, {}).get("cited_by_count", 0)
            monthly_rows.append(row_wc); monthly_rows.append(row_cb)
        monthly_df = pd.DataFrame(monthly_rows, columns=["source_id","display_name","metric"] + months_sorted)
    else:
        monthly_df = pd.DataFrame([{
            "source_id": "",
            "display_name": "",
            "metric": "info",
            "note": "Monthly aggregation disabled in app."
        }], columns=["source_id","display_name","metric","note"])

    # ── Package everything into a single ZIP (in memory) ──────────────────────
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("servers.csv", servers_df.to_csv(index=False))
        zf.writestr("server_yearly_trends.csv", yearly_df.to_csv(index=False))
        zf.writestr("server_monthly_trends.csv", monthly_df.to_csv(index=False))
        for row in servers_rows:
            sid = row.get("source_id", "unknown")
            raw = row.get("raw_json", "{}")
            zf.writestr(f"json/source_{sid}.json", raw)
        zf.writestr("json/selection_summary.json", json.dumps({
            "selected_source_ids": chosen_sids,
            "date_from": date_from,
            "date_to": date_to,
            "use_primary_location": use_primary_location,
            "use_host_venue": use_host_venue,
            "monthly_enabled": monthly_enabled
        }, ensure_ascii=False, indent=2))
    buf.seek(0)
    return buf.read(), servers_df, yearly_df, monthly_df

# ──────────────────────────────────────────────────────────────────────────────
# STEP 3 UI: Compact progress + last-N log + run button + previews
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### 3) Build & Download")
st.write(
    "When you’re ready, click **Fetch & Build ZIP**. You’ll see live logs and per-server progress. "
    "When done, you can preview the CSVs and download the ZIP."
)

# NEW: toggle to show/hide heavy progress UI (metrics, per-server panels, full logs)
show_progress_details = st.checkbox(
    "Show progress details (metrics, per-server panels, full logs)",
    value=False,
    help="Turn ON to see detailed build metrics and logs. OFF keeps the UI compact."
)

# (Optional) keep three metrics (placeholders passed into builder)
m1, m2, m3 = st.columns(3)

# Compact status row
c1, c2 = st.columns([1, 3])
with c1:
    compact_status = st.empty()   # one-line "Servers processed: X/Y"
with c2:
    compact_log = st.empty()      # rolling last-N lines
compact_progress = st.progress(0)

# Big global log: only render if details are shown
if show_progress_details:
    with st.expander("📜 Show build logs", expanded=False):
        overall_log_box = st.empty()
        overall_log_box.code("Logs will appear here…", language=None)
else:
    overall_log_box = st.empty()  # invisible placeholder

left, right = st.columns([1,1])
with left:
    run_btn = st.button(
        "🚀 Fetch & Build ZIP",
        key="btn_build_zip",
        disabled=not st.session_state.candidates_map,
        help="This will fetch selected Sources, build CSVs, and generate a ZIP."
    )

if run_btn:
    try:
        zip_bytes, servers_df, yearly_df, monthly_df = build_zip_from_selection(
            st.session_state.selections_map,
            sleep_s=float(sleep_s),
            mailto=mailto or None,
            date_from=norm_name(date_from) or None,
            date_to=norm_name(date_to) or None,
            use_primary_location=use_primary_location,
            use_host_venue=use_host_venue,
            monthly_enabled=monthly_enabled,
            overall_log=overall_log_box,
            metrics={"count": m1, "avg": m2, "eta": m3},
            compact_progress=compact_progress,
            compact_status=compact_status,
            compact_log=compact_log,
            compact_log_keep=5,  # show the last 5 lines in the compact log
            show_progress_details=show_progress_details,  # NEW
        )
        st.success("✅ Build complete! Preview below and download your ZIP.")

        st.markdown("#### Preview: servers.csv")
        st.write("Tip: `raw_json` is hidden in the preview for readability, but included in the ZIP.")
        preview_servers = servers_df.drop(columns=[c for c in servers_df.columns if c == "raw_json"])
        st.dataframe(preview_servers, use_container_width=True, height=300)

        st.markdown("#### Preview: server_yearly_trends.csv")
        st.dataframe(yearly_df, use_container_width=True, height=240)

        st.markdown("#### Preview: server_monthly_trends.csv")
        st.write(
            "If monthly aggregation was disabled, this file contains a short note. "
            "Enable monthly and specify a date range for detailed monthly counts."
        )
        st.dataframe(monthly_df.iloc[:, :40], use_container_width=True, height=240)

        # Create timestamp string like 2025-08-13_14-30
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        st.download_button(
            "⬇️ Download results ZIP",
            data=zip_bytes,
            file_name=f"openalex_preprint_servers_results_{timestamp}.zip",
            mime="application/zip",
            help="ZIP includes three CSVs and a json/ folder with raw records."
        )
    except Exception as e:
        st.error(f"Failed to build ZIP: {e}")

st.markdown("---")
st.markdown(
    """
**Helpful tips**
- If you run into rate limits, increase the **Sleep between API calls**.
- Use a **real email** in the Polite Pool field for smoother API access.
- Keep **Monthly OFF** for quick checks; use it for final analyses with a narrow date window.
"""
)
