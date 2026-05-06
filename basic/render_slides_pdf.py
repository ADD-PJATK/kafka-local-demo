#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import subprocess
from pathlib import Path
from typing import List


def _split_slides(md: str) -> List[str]:
    return [p.strip() for p in md.split("\n---\n") if p.strip()]


def _md_to_html(md: str) -> str:
    """
    Best-effort conversion:
    - If python-markdown is available, use it.
    - Otherwise render as <pre> (still printable to PDF).
    """
    try:
        import markdown  # type: ignore

        return markdown.markdown(
            md,
            extensions=[
                "extra",
                "tables",
                "fenced_code",
                "codehilite",
                "toc",
            ],
            output_format="html5",
        )
    except Exception:
        return f"<pre>{html.escape(md)}</pre>"


def build_html(slides: List[str], title: str) -> str:
    slide_divs = []
    for i, s in enumerate(slides, start=1):
        body = _md_to_html(s)
        slide_divs.append(
            f"""
            <section class="slide">
              <div class="slide-no">{i}/{len(slides)}</div>
              <div class="content">{body}</div>
            </section>
            """
        )

    slides_html = "\n".join(slide_divs)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{html.escape(title)}</title>
  <style>
    @page {{ size: A4 landscape; margin: 10mm; }}
    body {{
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color: #0b1020;
    }}
    .slide {{
      page-break-after: always;
      border: 1px solid #e5e7eb;
      border-radius: 14px;
      padding: 18px 22px;
      height: calc(100vh - 22mm);
      box-sizing: border-box;
      position: relative;
      overflow: hidden;
    }}
    .slide-no {{
      position: absolute;
      right: 16px;
      top: 12px;
      font-size: 12px;
      color: #6b7280;
    }}
    h1 {{ font-size: 34px; margin: 0 0 12px; }}
    h2 {{ font-size: 22px; margin: 0 0 10px; }}
    p, li {{ font-size: 18px; line-height: 1.35; }}
    code {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 16px;
    }}
    pre {{
      background: #0b1020;
      color: #e5e7eb;
      padding: 12px 14px;
      border-radius: 12px;
      overflow: hidden;
      white-space: pre-wrap;
    }}
    pre code {{ color: inherit; }}
    .content {{ max-width: 1200px; }}
  </style>
</head>
<body>
{slides_html}
</body>
</html>
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input slides markdown (split by ---)")
    ap.add_argument("--out", dest="out", required=True, help="Output PDF path")
    ap.add_argument("--title", dest="title", default="Kafka tutorial deck")
    args = ap.parse_args()

    in_path = Path(args.inp).resolve()
    out_path = Path(args.out).resolve()

    md = in_path.read_text(encoding="utf-8")
    slides = _split_slides(md)
    html_doc = build_html(slides, title=args.title)

    html_path = out_path.with_suffix(".html")
    html_path.write_text(html_doc, encoding="utf-8")

    subprocess.run(
        [
            "chromium",
            "--headless",
            "--disable-gpu",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            f"--print-to-pdf={out_path}",
            f"file://{html_path}",
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


if __name__ == "__main__":
    main()

