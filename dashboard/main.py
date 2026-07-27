"""CSV Pipeline Dashboard — FastAPI web UI."""
import os
import json
import csv
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
import plotly.express as px
import plotly.utils
import uvicorn

app = FastAPI(title="CSV Pipeline Dashboard", version="1.0.0")

BASE_DIR = Path(__file__).parent.parent
INCOMING = BASE_DIR / "incoming"
PROCESSED = BASE_DIR / "processed"
ARCHIVE = BASE_DIR / "archive"
DASHBOARD_DIR = Path(__file__).parent
TEMPLATES = DASHBOARD_DIR / "templates"
STATIC = DASHBOARD_DIR / "static"

TEMPLATES.mkdir(parents=True, exist_ok=True)
STATIC.mkdir(parents=True, exist_ok=True)

# Standalone Jinja2 (avoids Starlette cache bug with Jinja2 3.2+)
jinja = Environment(
    loader=FileSystemLoader(str(TEMPLATES)),
    autoescape=select_autoescape(["html", "xml"])
)


def render(name, **kwargs):
    """Render a Jinja2 template and return HTMLResponse."""
    template = jinja.get_template(name)
    html = template.render(**kwargs)
    return HTMLResponse(html)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard showing pipeline status and data."""
    incoming_files = list(INCOMING.glob("*.csv")) if INCOMING.exists() else []
    processed_files = list(PROCESSED.glob("*")) if PROCESSED.exists() else []
    archive_files = list(ARCHIVE.glob("*.csv")) if ARCHIVE.exists() else []

    stats = {
        "incoming": len(incoming_files),
        "processed": len(processed_files),
        "archived": len(archive_files),
        "total": len(incoming_files) + len(processed_files) + len(archive_files),
        "last_processed": max((f.stat().st_mtime for f in processed_files), default=0) if processed_files else 0,
    }

    return render("dashboard.html",
        stats=stats,
        incoming=[f.name for f in incoming_files[:10]],
        processed=[f.name for f in processed_files[:10]],
    )


@app.get("/api/stats")
async def api_stats():
    """JSON stats endpoint for real-time monitoring."""
    incoming_files = list(INCOMING.glob("*.csv")) if INCOMING.exists() else []
    processed_files = list(PROCESSED.glob("*")) if PROCESSED.exists() else []
    return {
        "incoming": len(incoming_files),
        "processed": len(processed_files),
        "timestamp": datetime.now().isoformat(),
    }


@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    """Upload a CSV to be processed by the pipeline."""
    INCOMING.mkdir(parents=True, exist_ok=True)
    dest = INCOMING / file.filename
    with open(dest, "wb") as f:
        content = await file.read()
        f.write(content)
    return {"status": "uploaded", "filename": file.filename, "size": len(content)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8500)
