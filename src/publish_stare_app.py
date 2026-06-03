from __future__ import annotations

import json
import re
from pathlib import Path
from shutil import copy2


ROOT = Path(__file__).resolve().parents[1]
APP_HTML = ROOT / "stare_app.html"
REPORT_JSON = ROOT / "reports" / "sector_dashboard.json"
REPORT_CSV = ROOT / "reports" / "sector_dashboard_top10.csv"
DOCS = ROOT / "docs"

DATA_RE = re.compile(
    r'(<script id="stare-data" type="application/json">)(.*?)(</script>)',
    re.DOTALL,
)


def load_compact_dashboard_json() -> str:
    data = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace(
        "</", "<\\/"
    )


def update_embedded_data(html: str, compact_json: str) -> str:
    if not DATA_RE.search(html):
        raise RuntimeError(f"Could not find embedded STARE data block in {APP_HTML}")
    return DATA_RE.sub(lambda m: f"{m.group(1)}{compact_json}{m.group(3)}", html, count=1)


def publish() -> None:
    if not APP_HTML.exists():
        raise RuntimeError(f"Missing app HTML: {APP_HTML}")
    if not REPORT_JSON.exists():
        raise RuntimeError(f"Missing dashboard JSON: {REPORT_JSON}")

    app_html = APP_HTML.read_text(encoding="utf-8")
    app_html = update_embedded_data(app_html, load_compact_dashboard_json())

    APP_HTML.write_text(app_html, encoding="utf-8")

    DOCS.mkdir(parents=True, exist_ok=True)
    (DOCS / "index.html").write_text(app_html, encoding="utf-8")
    copy2(REPORT_JSON, DOCS / "sector_dashboard.json")
    if REPORT_CSV.exists():
        copy2(REPORT_CSV, DOCS / "sector_dashboard_top10.csv")
    (DOCS / ".nojekyll").write_text("", encoding="utf-8")

    print("Published STARE app to:")
    print(f"- {APP_HTML.relative_to(ROOT)}")
    print(f"- {(DOCS / 'index.html').relative_to(ROOT)}")


if __name__ == "__main__":
    publish()
