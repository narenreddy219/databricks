#!/bin/bash

# Usage:
#   sh cpp_log_parser.sh application_logs.txt
#
# Output files:
#   file_events.psv
#   file_summary.psv
#   spark_events.psv
#   parquet_write_events.psv
#   archive_events.psv
#   completion_events.psv

LOG_FILE="$1"

if [ -z "$LOG_FILE" ]; then
  echo "Usage: sh cpp_log_parser.sh <application_log_file>"
  exit 1
fi

if [ ! -f "$LOG_FILE" ]; then
  echo "File not found: $LOG_FILE"
  exit 1
fi

echo "Reading log file: $LOG_FILE"

# -----------------------------------------------------------------------------
# 1. Extract all file processing events
# -----------------------------------------------------------------------------

grep -iE "processing file name|filter processing completed|kafka message|moving file|archive|parquet|write|save|DAGScheduler|Job|Stage|SparkContext|processFile|SUCCESS|FAILED" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","file_name","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  file="";

  # Extract file name from common patterns
  if (match(raw, /processing file name: [^ ]+/)) {
    file=substr(raw, RSTART+22, RLENGTH-22);
    event="PROCESSING_START";
  }
  else if (match(raw, /filter processing completed/)) {
    event="FILTER_PROCESSING_COMPLETED";
    # Try to get file from bracket field like [file.dat]
    if (match(raw, /\[[^]]+\]/)) {
      file=substr(raw, RSTART+1, RLENGTH-2);
    }
  }
  else if (match(raw, /kafka message/)) {
    event="KAFKA_AUDIT";
    if (match(raw, /"file_name":"[^"]+"/)) {
      file=substr(raw, RSTART+13, RLENGTH-14);
    }
  }
  else if (tolower(raw) ~ /moving file/) {
    event="HDFS_MOVE_ARCHIVE";
    if (match(raw, /\/[^ ]+/)) {
      file=substr(raw, RSTART, RLENGTH);
    }
  }
  else if (tolower(raw) ~ /parquet|write|save/) {
    event="PARQUET_WRITE_OR_SAVE";
  }
  else if (tolower(raw) ~ /dagscheduler|job|stage|sparkcontext/) {
    event="SPARK_JOB_STAGE_EVENT";
  }
  else if (tolower(raw) ~ /success/) {
    event="SUCCESS";
  }
  else if (tolower(raw) ~ /fail|error|exception/) {
    event="FAILED_OR_ERROR";
  }
  else {
    event="OTHER";
  }

  gsub(/\|/, " ", raw);
  print ts, thread, event, file, raw;
}
' > file_events.psv

echo "Created file_events.psv"


# -----------------------------------------------------------------------------
# 2. Extract Spark job/stage events
# -----------------------------------------------------------------------------

grep -iE "DAGScheduler|Job|Stage|SparkContext|SQLExecution|executor|task" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","spark_event_type","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;

  if (tolower(raw) ~ /job.*start|starting.*job|got job/) {
    event="SPARK_JOB_STARTED";
  }
  else if (tolower(raw) ~ /job.*finish|finished.*job/) {
    event="SPARK_JOB_FINISHED";
  }
  else if (tolower(raw) ~ /stage.*submit|submitting.*stage/) {
    event="STAGE_SUBMITTED";
  }
  else if (tolower(raw) ~ /stage.*finish|finished.*stage/) {
    event="STAGE_FINISHED";
  }
  else if (tolower(raw) ~ /dagscheduler/) {
    event="DAGSCHEDULER";
  }
  else {
    event="SPARK_OTHER";
  }

  gsub(/\|/, " ", raw);
  print ts, thread, event, raw;
}
' > spark_events.psv

echo "Created spark_events.psv"


# -----------------------------------------------------------------------------
# 3. Extract Parquet/write/save events
# -----------------------------------------------------------------------------

grep -iE "parquet|write|save|InsertIntoHadoopFsRelation|FileOutputCommitter|commit" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;

  if (tolower(raw) ~ /parquet/) {
    event="PARQUET";
  }
  else if (tolower(raw) ~ /write|save/) {
    event="WRITE_SAVE";
  }
  else if (tolower(raw) ~ /commit/) {
    event="COMMIT";
  }
  else {
    event="WRITE_OTHER";
  }

  gsub(/\|/, " ", raw);
  print ts, thread, event, raw;
}
' > parquet_write_events.psv

echo "Created parquet_write_events.psv"


# -----------------------------------------------------------------------------
# 4. Extract archive/move/rename events
# -----------------------------------------------------------------------------

grep -iE "moving file|move|rename|archive|delete|_peak_mode_processing" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;

  if (tolower(raw) ~ /rename/) {
    event="RENAME";
  }
  else if (tolower(raw) ~ /moving file|move/) {
    event="MOVE";
  }
  else if (tolower(raw) ~ /archive/) {
    event="ARCHIVE";
  }
  else if (tolower(raw) ~ /delete/) {
    event="DELETE";
  }
  else if (tolower(raw) ~ /_peak_mode_processing/) {
    event="PEAK_MODE_PROCESSING";
  }
  else {
    event="HDFS_OTHER";
  }

  gsub(/\|/, " ", raw);
  print ts, thread, event, raw;
}
' > archive_events.psv

echo "Created archive_events.psv"


# -----------------------------------------------------------------------------
# 5. Extract completion/success/failure events
# -----------------------------------------------------------------------------

grep -iE "filter processing completed|SUCCESS|FAILED|completed|done|kafka message|Exception|ERROR" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","file_name","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  file="";

  if (tolower(raw) ~ /filter processing completed/) {
    event="FILTER_COMPLETED";
    if (match(raw, /\[[^]]+\]/)) {
      file=substr(raw, RSTART+1, RLENGTH-2);
    }
  }
  else if (tolower(raw) ~ /kafka message/ && tolower(raw) ~ /success/) {
    event="KAFKA_SUCCESS";
    if (match(raw, /"file_name":"[^"]+"/)) {
      file=substr(raw, RSTART+13, RLENGTH-14);
    }
  }
  else if (tolower(raw) ~ /success/) {
    event="SUCCESS";
  }
  else if (tolower(raw) ~ /failed|exception|error/) {
    event="FAILED_OR_ERROR";
  }
  else if (tolower(raw) ~ /done|completed/) {
    event="COMPLETED";
  }
  else {
    event="OTHER_COMPLETION";
  }

  gsub(/\|/, " ", raw);
  print ts, thread, event, file, raw;
}
' > completion_events.psv

echo "Created completion_events.psv"


# -----------------------------------------------------------------------------
# 6. Thread usage summary
# -----------------------------------------------------------------------------

grep -i "processing file name" "$LOG_FILE" \
| awk '{print $2}' \
| sort \
| uniq -c \
| awk 'BEGIN{OFS="|"; print "processing_log_count","thread"} {print $1,$2}' \
> thread_summary.psv

echo "Created thread_summary.psv"


# -----------------------------------------------------------------------------
# 7. File-level summary: start, filter complete, kafka success, archive events
#    This is simple and based on file name visibility in log lines.
# -----------------------------------------------------------------------------

awk '
BEGIN {
  OFS="|";
}
{
  raw=$0;
  ts=$1;
  thread=$2;
  file="";

  if (match(raw, /processing file name: [^ ]+/)) {
    file=substr(raw, RSTART+22, RLENGTH-22);
    start[file]=ts;
    start_thread[file]=thread;
  }

  if (tolower(raw) ~ /filter processing completed/) {
    if (match(raw, /\[[^]]+\]/)) {
      file=substr(raw, RSTART+1, RLENGTH-2);
      filter_done[file]=ts;
    }
  }

  if (tolower(raw) ~ /kafka message/ && tolower(raw) ~ /success/) {
    if (match(raw, /"file_name":"[^"]+"/)) {
      file=substr(raw, RSTART+13, RLENGTH-14);
      kafka_success[file]=ts;
    }
  }

  if (tolower(raw) ~ /moving file/) {
    for (f in start) {
      if (index(raw, f) > 0) {
        archive_time[f]=ts;
      }
    }
  }

  if (tolower(raw) ~ /parquet|write|save/) {
    for (f in start) {
      if (index(raw, f) > 0) {
        parquet_time[f]=ts;
      }
    }
  }
}
END {
  print "file_name","start_time","start_thread","filter_completed_time","parquet_write_time","archive_time","kafka_success_time";
  for (f in start) {
    print f,start[f],start_thread[f],filter_done[f],parquet_time[f],archive_time[f],kafka_success[f];
  }
}
' "$LOG_FILE" > file_summary.psv

echo "Created file_summary.psv"


echo ""
echo "Done."
echo "Generated files:"
echo "  file_events.psv"
echo "  file_summary.psv"
echo "  spark_events.psv"
echo "  parquet_write_events.psv"
echo "  archive_events.psv"
echo "  completion_events.psv"
echo "  thread_summary.psv"
