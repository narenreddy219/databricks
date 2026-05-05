#!/usr/bin/env python3
"""
CPP / DataPreProcessor Log Timeline Parser

Purpose:
- Parse CPP application logs.
- Identify per-file processing timeline.
- Include processing start, filter/DMW completion, parquet/write logs, Spark job/stage events,
  archive/move logs, Kafka/audit/success/failure completion events.
- Output pipe-separated files and an Excel workbook.

Usage examples:
  python cpp_log_timeline_parser.py --log application_logs.txt --out-prefix cpp_app_analysis
  python cpp_log_timeline_parser.py --log application_logs.txt --year 2026 --out-prefix cpp_app_analysis

Outputs:
  <out-prefix>_file_summary.psv
  <out-prefix>_file_events.psv
  <out-prefix>_spark_events.psv
  <out-prefix>_parquet_write_events.psv
  <out-prefix>_raw_relevant_events.psv
  <out-prefix>.xlsx

Notes:
- If your log timestamp does not include year, pass --year YYYY.
- Excel output requires pandas + openpyxl. Pipe-separated outputs are always created.
"""

import argparse
import csv
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# -----------------------------
# Regex patterns
# -----------------------------
TS_RE = re.compile(r"(?P<ts>(?:\d{4}-)?\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)")
THREAD_RE = re.compile(r"\[(T-\d+)\]")
APP_RE = re.compile(r"application_[0-9]+_[0-9]+")

# File patterns observed in your logs
FILE_PATTERNS = [
    re.compile(r"processing file name\s*:?\s*(?P<file>[^\s,;]+)", re.IGNORECASE),
    re.compile(r"Initializing DataFrame for\s+(?P<file>[^\s,;]+)", re.IGNORECASE),
    re.compile(r"filter processing completed.*?(?P<file>[^\s,;]+\.(?:dat|txt|csv|psv|gz|zip|parquet|orc|json)(?:\.gz)?)", re.IGNORECASE),
    re.compile(r"file_name['\"]?\s*[:=]\s*['\"](?P<file>[^'\"]+)['\"]", re.IGNORECASE),
    re.compile(r"moving file from\s+(?P<src>\S+)\s+to\s+(?P<tgt>\S+)", re.IGNORECASE),
    re.compile(r"moving file\s+(?P<src>\S+)\s+to\s+(?P<tgt>\S+).*done", re.IGNORECASE),
]

# Event categorization
EVENT_RULES = [
    ("FILE_PROCESSING_START", re.compile(r"processing file name", re.IGNORECASE)),
    ("DATAFRAME_INIT", re.compile(r"Initializing DataFrame", re.IGNORECASE)),
    ("FILTER_DMW_COMPLETED", re.compile(r"filter processing completed|processDMW.*completed", re.IGNORECASE)),
    ("PARQUET_WRITE", re.compile(r"parquet|savePrq|savePrqt|write parquet|write.*parquet|InsertIntoHadoopFsRelation|FileOutputCommitter", re.IGNORECASE)),
    ("SPARK_JOB_EVENT", re.compile(r"DAGScheduler|Job [0-9]+|Job started|Job finished|Submitting job|ResultStage|ShuffleMapStage|SparkContext|SQLExecution|Stage [0-9]+", re.IGNORECASE)),
    ("HDFS_RENAME_MOVE_ARCHIVE", re.compile(r"rename|moving file|moveFile|archive|_peak_mode_processing|delete", re.IGNORECASE)),
    ("KAFKA_AUDIT_SUCCESS", re.compile(r"kafka message.*SUCCESS|status['\"]?\s*[:=]\s*['\"]SUCCESS", re.IGNORECASE)),
    ("FAILURE_ERROR", re.compile(r"ERROR|Exception|FAILED|status['\"]?\s*[:=]\s*['\"]FAILED", re.IGNORECASE)),
    ("COMPLETION", re.compile(r"completed|success|done", re.IGNORECASE)),
]

COMPLETION_TYPES = {"FILTER_DMW_COMPLETED", "KAFKA_AUDIT_SUCCESS", "HDFS_RENAME_MOVE_ARCHIVE", "COMPLETION"}


def parse_timestamp(line: str, default_year: int | None):
    m = TS_RE.search(line)
    if not m:
        return None, ""
    raw = m.group("ts")
    try:
        if re.match(r"^\d{4}-", raw):
            # Example: 2026-05-05T05:43:53.236
            return datetime.fromisoformat(raw), raw
        if default_year is None:
            return None, raw
        # Example: 05-05T13:12:44.588 -> 2026-05-05T13:12:44.588
        full = f"{default_year}-{raw}"
        return datetime.fromisoformat(full), raw
    except Exception:
        return None, raw


def get_thread(line: str) -> str:
    m = THREAD_RE.search(line)
    return m.group(1) if m else ""


def get_application_id(line: str) -> str:
    m = APP_RE.search(line)
    return m.group(0) if m else ""


def basename_from_path(path_text: str) -> str:
    if not path_text:
        return ""
    # Handles HDFS and Windows-like paths by splitting on / and \\
    return re.split(r"[\\/]", path_text.rstrip(" ,;"))[-1]


def extract_file(line: str) -> tuple[str, str, str]:
    """Return (file_name, source_path, target_path)."""
    for pat in FILE_PATTERNS:
        m = pat.search(line)
        if not m:
            continue
        gd = m.groupdict()
        if "file" in gd and gd.get("file"):
            return basename_from_path(gd["file"]), "", ""
        if "src" in gd and gd.get("src"):
            src = gd.get("src", "")
            tgt = gd.get("tgt", "")
            return basename_from_path(src), src, tgt
    return "", "", ""


def categorize(line: str) -> list[str]:
    types = []
    for name, pat in EVENT_RULES:
        if pat.search(line):
            types.append(name)
    return types


def sec_diff(start, end):
    if start is None or end is None:
        return ""
    return round((end - start).total_seconds(), 3)


def min_diff(start, end):
    s = sec_diff(start, end)
    if s == "":
        return ""
    return round(s / 60.0, 3)


def write_psv(path: Path, rows: list[dict], fieldnames: list[str]):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="|", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Parse CPP logs and create per-file timeline analysis.")
    parser.add_argument("--log", required=True, help="Input application log file path")
    parser.add_argument("--out-prefix", default="cpp_app_analysis", help="Output file prefix")
    parser.add_argument("--year", type=int, default=None, help="Default year when log timestamps omit year, e.g. 2026")
    parser.add_argument("--max-raw-line", type=int, default=2000, help="Max raw log line length to keep in output")
    args = parser.parse_args()

    log_path = Path(args.log)
    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")

    out_prefix = Path(args.out_prefix)
    out_dir = out_prefix.parent if str(out_prefix.parent) != "." else Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)

    relevant_events = []
    file_events = []
    spark_events = []
    parquet_events = []
    all_lines_count = 0

    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            all_lines_count += 1
            line = line.rstrip("\n")
            event_types = categorize(line)
            file_name, src_path, tgt_path = extract_file(line)
            timestamp, raw_ts = parse_timestamp(line, args.year)
            thread = get_thread(line)
            app_id = get_application_id(line)

            # Keep all relevant Spark/parquet/completion/file-related records
            if event_types or file_name:
                base = {
                    "line_no": line_no,
                    "timestamp": timestamp.isoformat(sep=" ") if timestamp else "",
                    "raw_timestamp": raw_ts,
                    "application_id": app_id,
                    "thread": thread,
                    "file_name": file_name,
                    "source_path": src_path,
                    "target_path": tgt_path,
                    "event_types": ",".join(event_types),
                    "raw_log": line[: args.max_raw_line],
                }
                relevant_events.append(base)

                if file_name:
                    for et in event_types or ["FILE_RELATED_UNKNOWN"]:
                        row = dict(base)
                        row["event_type"] = et
                        file_events.append(row)

                if "SPARK_JOB_EVENT" in event_types:
                    spark_events.append(base)

                if "PARQUET_WRITE" in event_types:
                    parquet_events.append(base)

    # Build per-file summary
    by_file = defaultdict(list)
    for ev in file_events:
        by_file[ev["file_name"]].append(ev)

    file_summary = []
    for file_name, events in by_file.items():
        # sort by parsed timestamp then line number
        def sort_key(e):
            ts = e.get("timestamp") or "9999-12-31 23:59:59"
            return (ts, int(e.get("line_no", 0)))
        events_sorted = sorted(events, key=sort_key)

        def first_event(event_type):
            for e in events_sorted:
                if e["event_type"] == event_type:
                    return e
            return None

        def last_event(event_type):
            found = None
            for e in events_sorted:
                if e["event_type"] == event_type:
                    found = e
            return found

        first_any = events_sorted[0] if events_sorted else None
        last_any = events_sorted[-1] if events_sorted else None
        start_ev = first_event("FILE_PROCESSING_START") or first_any
        df_ev = first_event("DATAFRAME_INIT")
        filter_ev = first_event("FILTER_DMW_COMPLETED")
        parquet_first = first_event("PARQUET_WRITE")
        parquet_last = last_event("PARQUET_WRITE")
        archive_first = first_event("HDFS_RENAME_MOVE_ARCHIVE")
        archive_last = last_event("HDFS_RENAME_MOVE_ARCHIVE")
        kafka_ev = last_event("KAFKA_AUDIT_SUCCESS")
        fail_ev = last_event("FAILURE_ERROR")

        # completion is last of known completion/success/archive/filter events, else last_any
        completion_candidates = [e for e in events_sorted if e["event_type"] in COMPLETION_TYPES]
        completion_ev = completion_candidates[-1] if completion_candidates else last_any

        def parse_out_ts(e):
            if not e or not e.get("timestamp"):
                return None
            try:
                return datetime.fromisoformat(e["timestamp"])
            except Exception:
                return None

        start_ts = parse_out_ts(start_ev)
        completion_ts = parse_out_ts(completion_ev)
        filter_ts = parse_out_ts(filter_ev)
        kafka_ts = parse_out_ts(kafka_ev)
        archive_start_ts = parse_out_ts(archive_first)
        archive_end_ts = parse_out_ts(archive_last)
        parquet_start_ts = parse_out_ts(parquet_first)
        parquet_end_ts = parse_out_ts(parquet_last)

        # Count global Spark events that occurred between file start and completion.
        spark_between = 0
        if start_ts and completion_ts:
            for se in spark_events:
                try:
                    st = datetime.fromisoformat(se["timestamp"]) if se.get("timestamp") else None
                    if st and start_ts <= st <= completion_ts:
                        spark_between += 1
                except Exception:
                    pass

        thread_names = sorted({e.get("thread", "") for e in events_sorted if e.get("thread")})
        event_type_counts = defaultdict(int)
        for e in events_sorted:
            event_type_counts[e["event_type"]] += 1

        status = "UNKNOWN"
        if fail_ev:
            status = "FAILED_OR_ERROR_SEEN"
        elif kafka_ev:
            status = "SUCCESS_AUDIT_SEEN"
        elif completion_ev:
            status = "COMPLETION_SEEN"

        file_summary.append({
            "file_name": file_name,
            "status_inferred": status,
            "first_seen_time": first_any.get("timestamp", "") if first_any else "",
            "processing_start_time": start_ev.get("timestamp", "") if start_ev else "",
            "dataframe_init_time": df_ev.get("timestamp", "") if df_ev else "",
            "filter_dmw_completed_time": filter_ev.get("timestamp", "") if filter_ev else "",
            "parquet_write_first_time": parquet_first.get("timestamp", "") if parquet_first else "",
            "parquet_write_last_time": parquet_last.get("timestamp", "") if parquet_last else "",
            "archive_first_time": archive_first.get("timestamp", "") if archive_first else "",
            "archive_last_time": archive_last.get("timestamp", "") if archive_last else "",
            "kafka_success_time": kafka_ev.get("timestamp", "") if kafka_ev else "",
            "completion_time": completion_ev.get("timestamp", "") if completion_ev else "",
            "total_duration_minutes_start_to_completion": min_diff(start_ts, completion_ts),
            "duration_minutes_start_to_filter_done": min_diff(start_ts, filter_ts),
            "duration_minutes_filter_done_to_kafka_success": min_diff(filter_ts, kafka_ts),
            "duration_minutes_archive_start_to_archive_end": min_diff(archive_start_ts, archive_end_ts),
            "duration_minutes_parquet_first_to_last": min_diff(parquet_start_ts, parquet_end_ts),
            "spark_events_between_start_completion": spark_between,
            "thread_names": ",".join(thread_names),
            "thread_count_seen": len(thread_names),
            "file_event_count": len(events_sorted),
            "parquet_write_event_count": event_type_counts.get("PARQUET_WRITE", 0),
            "spark_job_event_count_with_file_name": event_type_counts.get("SPARK_JOB_EVENT", 0),
            "archive_move_event_count": event_type_counts.get("HDFS_RENAME_MOVE_ARCHIVE", 0),
            "completion_event_count": sum(event_type_counts.get(t, 0) for t in COMPLETION_TYPES),
            "first_line_no": first_any.get("line_no", "") if first_any else "",
            "last_line_no": last_any.get("line_no", "") if last_any else "",
        })

    file_summary = sorted(file_summary, key=lambda r: (r.get("processing_start_time") or r.get("first_seen_time"), r["file_name"]))

    # Global summary rows
    distinct_threads = sorted({ev["thread"] for ev in relevant_events if ev.get("thread")})
    distinct_files = sorted(by_file.keys())
    global_summary = [{
        "metric": "input_log_file",
        "value": str(log_path),
    }, {
        "metric": "total_log_lines_read",
        "value": all_lines_count,
    }, {
        "metric": "distinct_files_seen",
        "value": len(distinct_files),
    }, {
        "metric": "distinct_threads_seen",
        "value": len(distinct_threads),
    }, {
        "metric": "spark_event_lines",
        "value": len(spark_events),
    }, {
        "metric": "parquet_write_event_lines",
        "value": len(parquet_events),
    }, {
        "metric": "relevant_event_lines",
        "value": len(relevant_events),
    }]

    # Write pipe-separated outputs
    summary_fields = [
        "file_name", "status_inferred", "first_seen_time", "processing_start_time", "dataframe_init_time",
        "filter_dmw_completed_time", "parquet_write_first_time", "parquet_write_last_time",
        "archive_first_time", "archive_last_time", "kafka_success_time", "completion_time",
        "total_duration_minutes_start_to_completion", "duration_minutes_start_to_filter_done",
        "duration_minutes_filter_done_to_kafka_success", "duration_minutes_archive_start_to_archive_end",
        "duration_minutes_parquet_first_to_last", "spark_events_between_start_completion", "thread_names",
        "thread_count_seen", "file_event_count", "parquet_write_event_count", "spark_job_event_count_with_file_name",
        "archive_move_event_count", "completion_event_count", "first_line_no", "last_line_no"
    ]
    event_fields = ["line_no", "timestamp", "raw_timestamp", "application_id", "thread", "file_name", "source_path", "target_path", "event_type", "event_types", "raw_log"]
    raw_fields = ["line_no", "timestamp", "raw_timestamp", "application_id", "thread", "file_name", "source_path", "target_path", "event_types", "raw_log"]

    write_psv(Path(f"{out_prefix}_file_summary.psv"), file_summary, summary_fields)
    write_psv(Path(f"{out_prefix}_file_events.psv"), file_events, event_fields)
    write_psv(Path(f"{out_prefix}_spark_events.psv"), spark_events, raw_fields)
    write_psv(Path(f"{out_prefix}_parquet_write_events.psv"), parquet_events, raw_fields)
    write_psv(Path(f"{out_prefix}_raw_relevant_events.psv"), relevant_events, raw_fields)
    write_psv(Path(f"{out_prefix}_global_summary.psv"), global_summary, ["metric", "value"])

    # Excel output - optional dependency
    xlsx_path = Path(f"{out_prefix}.xlsx")
    try:
        import pandas as pd
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            pd.DataFrame(global_summary).to_excel(writer, sheet_name="Global Summary", index=False)
            pd.DataFrame(file_summary).to_excel(writer, sheet_name="File Summary", index=False)
            pd.DataFrame(file_events).to_excel(writer, sheet_name="File Events", index=False)
            pd.DataFrame(spark_events).to_excel(writer, sheet_name="Spark Events", index=False)
            pd.DataFrame(parquet_events).to_excel(writer, sheet_name="Parquet Write Events", index=False)
            pd.DataFrame(relevant_events).to_excel(writer, sheet_name="Raw Relevant Events", index=False)

            # Basic formatting
            wb = writer.book
            for ws in wb.worksheets:
                ws.freeze_panes = "A2"
                for col_cells in ws.columns:
                    max_len = 0
                    col_letter = col_cells[0].column_letter
                    for cell in col_cells[:200]:
                        value = str(cell.value) if cell.value is not None else ""
                        max_len = max(max_len, min(len(value), 80))
                    ws.column_dimensions[col_letter].width = max(12, min(max_len + 2, 60))
                for cell in ws[1]:
                    cell.style = "Headline 4"
        excel_msg = f"Excel written: {xlsx_path}"
    except Exception as e:
        excel_msg = f"Excel not written because pandas/openpyxl failed or is unavailable: {e}"

    print("CPP log parsing completed")
    print(f"Files found: {len(distinct_files)}")
    print(f"Threads found: {len(distinct_threads)}")
    print(f"Relevant events: {len(relevant_events)}")
    print(f"Spark events: {len(spark_events)}")
    print(f"Parquet/write events: {len(parquet_events)}")
    print(f"Output prefix: {out_prefix}")
    print(excel_msg)


if __name__ == "__main__":
    main()
