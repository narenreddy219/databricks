#!/bin/bash

# =============================================================================
# CPP Deep Dive Log Parser
# -----------------------------------------------------------------------------
# Purpose:
# Extract file-level details from CPP/DataPreProcessor application logs:
# - file processing start events
# - filter/DMW/transformation events
# - parquet/PQT/write/save events
# - Spark job/stage events
# - archive/move events
# - Kafka/audit/success/failure events
# - possible file size/record count details if present in logs
#
# Usage:
#   sh cpp_deep_dive_log_parser.sh application_logs.txt
#
# Output:
#   01_file_processing_events.psv
#   02_file_lifecycle_summary.psv
#   03_transformation_filter_events.psv
#   04_parquet_write_events.psv
#   05_spark_job_stage_events.psv
#   06_archive_move_events.psv
#   07_kafka_audit_events.psv
#   08_file_size_record_count_events.psv
#   09_thread_summary.psv
#   10_all_relevant_events.psv
# =============================================================================

LOG_FILE="$1"

if [ -z "$LOG_FILE" ]; then
  echo "Usage: sh cpp_deep_dive_log_parser.sh <application_log_file>"
  exit 1
fi

if [ ! -f "$LOG_FILE" ]; then
  echo "File not found: $LOG_FILE"
  exit 1
fi

echo "Reading log file: $LOG_FILE"

# -----------------------------------------------------------------------------
# Helper extraction note:
# Timestamp assumed as first column.
# Thread assumed as second column if available, for example [T-3534].
# -----------------------------------------------------------------------------

# =============================================================================
# 01. File processing events
# =============================================================================

grep -iE "processing file name|processFile|processing file|completed file|file processing completed|SUCCESS|FAILED|Exception|ERROR" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","file_name","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);
  file="";
  event="OTHER_FILE_EVENT";

  if (match(raw, /processing file name: [^ ]+/)) {
    file=substr(raw, RSTART+22, RLENGTH-22);
    event="PROCESSING_START";
  }
  else if (lower ~ /processfile/) {
    event="PROCESSFILE_EVENT";
  }
  else if (lower ~ /filter processing completed/) {
    event="FILTER_PROCESSING_COMPLETED";
    if (match(raw, /\[[^]]+\]/)) {
      file=substr(raw, RSTART+1, RLENGTH-2);
    }
  }
  else if (lower ~ /success/) {
    event="SUCCESS_EVENT";
    if (match(raw, /"file_name":"[^"]+"/)) {
      file=substr(raw, RSTART+13, RLENGTH-14);
    }
  }
  else if (lower ~ /failed|exception|error/) {
    event="FAILED_OR_ERROR";
    if (match(raw, /"file_name":"[^"]+"/)) {
      file=substr(raw, RSTART+13, RLENGTH-14);
    }
  }

  gsub(/\|/, " ", raw);
  print ts,thread,event,file,raw;
}
' > 01_file_processing_events.psv

echo "Created 01_file_processing_events.psv"


# =============================================================================
# 02. File lifecycle summary
# =============================================================================

awk '
BEGIN {
  OFS="|";
}

function clean_file(f) {
  gsub(/"/, "", f);
  gsub(/,/, "", f);
  gsub(/\]/, "", f);
  gsub(/\[/, "", f);
  gsub(/\r/, "", f);
  return f;
}

{
  raw=$0;
  lower=tolower(raw);
  ts=$1;
  thread=$2;
  file="";

  # Processing start
  if (match(raw, /processing file name: [^ ]+/)) {
    file=substr(raw, RSTART+22, RLENGTH-22);
    file=clean_file(file);
    if (!(file in processing_start)) {
      processing_start[file]=ts;
      start_thread[file]=thread;
    }
  }

  # Filter completed
  if (lower ~ /filter processing completed/) {
    if (match(raw, /\[[^]]+\]/)) {
      file=substr(raw, RSTART+1, RLENGTH-2);
      file=clean_file(file);
      if (!(file in filter_completed)) {
        filter_completed[file]=ts;
      }
    }
  }

  # Transformation / DMW / filter events
  if (lower ~ /dmw|transformation|transform|filtercriteria|filter criteria|aggregation|generatedfeed|xml|rule/) {
    for (f in processing_start) {
      if (index(raw, f) > 0) {
        transformation_event_count[f]++;
        if (!(f in transformation_first)) {
          transformation_first[f]=ts;
        }
        transformation_last[f]=ts;
      }
    }
  }

  # Parquet / PQT / write / save events
  if (lower ~ /parquet|pqt|write|save|insertintohadoopfsrelation|fileoutputcommitter|commit|part-/) {
    for (f in processing_start) {
      if (index(raw, f) > 0) {
        parquet_event_count[f]++;
        if (!(f in parquet_first)) {
          parquet_first[f]=ts;
        }
        parquet_last[f]=ts;
      }
    }
  }

  # Spark job/stage events
  if (lower ~ /dagscheduler|job|stage|sparkcontext|sqlexecution|executor|task/) {
    for (f in processing_start) {
      if (index(raw, f) > 0) {
        spark_event_count[f]++;
        if (!(f in spark_first)) {
          spark_first[f]=ts;
        }
        spark_last[f]=ts;
      }
    }
  }

  # Archive/move/rename/delete events
  if (lower ~ /moving file|move|rename|archive|delete|_peak_mode_processing/) {
    for (f in processing_start) {
      if (index(raw, f) > 0) {
        archive_event_count[f]++;
        if (!(f in archive_first)) {
          archive_first[f]=ts;
        }
        archive_last[f]=ts;
      }
    }
  }

  # Kafka/audit success/failure
  if (lower ~ /kafka message|audit|insert/) {
    if (match(raw, /"file_name":"[^"]+"/)) {
      file=substr(raw, RSTART+13, RLENGTH-14);
      file=clean_file(file);
      kafka_event_count[file]++;
      if (!(file in kafka_first)) {
        kafka_first[file]=ts;
      }
      kafka_last[file]=ts;

      if (lower ~ /success/) {
        kafka_success[file]=ts;
      }
      if (lower ~ /failed|failure|error/) {
        kafka_failed[file]=ts;
      }
    }
  }

  # Record count if present
  if (lower ~ /record_count|record count|records/) {
    for (f in processing_start) {
      if (index(raw, f) > 0) {
        record_event_count[f]++;
        record_last_line[f]=raw;
      }
    }
  }

  # File size if present
  if (lower ~ /file size|filesize|bytes|length|content length|len=/) {
    for (f in processing_start) {
      if (index(raw, f) > 0) {
        size_event_count[f]++;
        size_last_line[f]=raw;
      }
    }
  }
}

END {
  print "file_name",
        "start_thread",
        "processing_start",
        "filter_completed",
        "transformation_first",
        "transformation_last",
        "parquet_first",
        "parquet_last",
        "spark_first",
        "spark_last",
        "archive_first",
        "archive_last",
        "kafka_first",
        "kafka_last",
        "kafka_success",
        "kafka_failed",
        "transformation_event_count",
        "parquet_event_count",
        "spark_event_count",
        "archive_event_count",
        "kafka_event_count",
        "record_event_count",
        "size_event_count",
        "record_last_line",
        "size_last_line";

  for (f in processing_start) {
    rec=record_last_line[f];
    size=size_last_line[f];
    gsub(/\|/, " ", rec);
    gsub(/\|/, " ", size);

    print f,
          start_thread[f],
          processing_start[f],
          filter_completed[f],
          transformation_first[f],
          transformation_last[f],
          parquet_first[f],
          parquet_last[f],
          spark_first[f],
          spark_last[f],
          archive_first[f],
          archive_last[f],
          kafka_first[f],
          kafka_last[f],
          kafka_success[f],
          kafka_failed[f],
          transformation_event_count[f]+0,
          parquet_event_count[f]+0,
          spark_event_count[f]+0,
          archive_event_count[f]+0,
          kafka_event_count[f]+0,
          record_event_count[f]+0,
          size_event_count[f]+0,
          rec,
          size;
  }
}
' "$LOG_FILE" > 02_file_lifecycle_summary.psv

echo "Created 02_file_lifecycle_summary.psv"


# =============================================================================
# 03. Transformation / filter / DMW events
# =============================================================================

grep -iE "transformation|transform|dmw|filter|filtercriteria|filter criteria|aggregation|aggregate|generatedfeed|xml|mapping|rule|expression|Initializing DataFrame|filter processing completed" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);

  if (lower ~ /filter processing completed/) {
    event="FILTER_COMPLETED";
  }
  else if (lower ~ /initializing dataframe/) {
    event="DATAFRAME_INIT";
  }
  else if (lower ~ /dmw/) {
    event="DMW";
  }
  else if (lower ~ /transformation|transform/) {
    event="TRANSFORMATION";
  }
  else if (lower ~ /filtercriteria|filter criteria/) {
    event="FILTER_CRITERIA";
  }
  else if (lower ~ /aggregation|aggregate/) {
    event="AGGREGATION";
  }
  else if (lower ~ /generatedfeed|xml/) {
    event="GENERATED_FEED_XML";
  }
  else {
    event="OTHER_TRANSFORMATION_FILTER";
  }

  gsub(/\|/, " ", raw);
  print ts,thread,event,raw;
}
' > 03_transformation_filter_events.psv

echo "Created 03_transformation_filter_events.psv"


# =============================================================================
# 04. Parquet / PQT / write events
# =============================================================================

grep -iE "parquet|pqt|write|save|output path|target path|InsertIntoHadoopFsRelation|FileOutputCommitter|commit|committed|_temporary|part-" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","path_or_file_hint","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);
  hint="";

  if (lower ~ /parquet/) {
    event="PARQUET";
  }
  else if (lower ~ /pqt/) {
    event="PQT";
  }
  else if (lower ~ /write|save/) {
    event="WRITE_SAVE";
  }
  else if (lower ~ /commit|committed/) {
    event="COMMIT";
  }
  else {
    event="OTHER_WRITE";
  }

  if (match(raw, /hdfs:\/\/[^ ]+/)) {
    hint=substr(raw, RSTART, RLENGTH);
  }
  else if (match(raw, /\/[^ ]+/)) {
    hint=substr(raw, RSTART, RLENGTH);
  }

  gsub(/\|/, " ", raw);
  print ts,thread,event,hint,raw;
}
' > 04_parquet_write_events.psv

echo "Created 04_parquet_write_events.psv"


# =============================================================================
# 05. Spark job/stage events
# =============================================================================

grep -iE "DAGScheduler|Job|Stage|Submitting|finished|ResultStage|ShuffleMapStage|SparkContext|SQLExecution|FileScan|broadcast|executor|task|shuffle|spill|GC" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","spark_event_type","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);

  if (lower ~ /job.*start|starting.*job|got job/) {
    event="SPARK_JOB_STARTED";
  }
  else if (lower ~ /job.*finish|finished.*job/) {
    event="SPARK_JOB_FINISHED";
  }
  else if (lower ~ /stage.*submit|submitting.*stage/) {
    event="STAGE_SUBMITTED";
  }
  else if (lower ~ /stage.*finish|finished.*stage/) {
    event="STAGE_FINISHED";
  }
  else if (lower ~ /shuffle/) {
    event="SHUFFLE";
  }
  else if (lower ~ /spill/) {
    event="SPILL";
  }
  else if (lower ~ /gc|garbage/) {
    event="GC";
  }
  else if (lower ~ /executor/) {
    event="EXECUTOR";
  }
  else if (lower ~ /task/) {
    event="TASK";
  }
  else if (lower ~ /dagscheduler/) {
    event="DAGSCHEDULER";
  }
  else {
    event="SPARK_OTHER";
  }

  gsub(/\|/, " ", raw);
  print ts,thread,event,raw;
}
' > 05_spark_job_stage_events.psv

echo "Created 05_spark_job_stage_events.psv"


# =============================================================================
# 06. Archive / move / rename events
# =============================================================================

grep -iE "moving file|move|rename|archive|delete|copy|_peak_mode_processing|source to archive|done" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","source_or_target_hint","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);
  hint="";

  if (lower ~ /rename/) {
    event="RENAME";
  }
  else if (lower ~ /moving file|move/) {
    event="MOVE";
  }
  else if (lower ~ /archive/) {
    event="ARCHIVE";
  }
  else if (lower ~ /copy/) {
    event="COPY";
  }
  else if (lower ~ /delete/) {
    event="DELETE";
  }
  else if (lower ~ /_peak_mode_processing/) {
    event="PEAK_MODE_PROCESSING";
  }
  else {
    event="HDFS_OTHER";
  }

  if (match(raw, /hdfs:\/\/[^ ]+/)) {
    hint=substr(raw, RSTART, RLENGTH);
  }
  else if (match(raw, /\/[^ ]+/)) {
    hint=substr(raw, RSTART, RLENGTH);
  }

  gsub(/\|/, " ", raw);
  print ts,thread,event,hint,raw;
}
' > 06_archive_move_events.psv

echo "Created 06_archive_move_events.psv"


# =============================================================================
# 07. Kafka / audit events
# =============================================================================

grep -iE "kafka message|audit|INSERT|status|SUCCESS|FAILED|record_count|record count|file_name|recon_id|reconid" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","recon_id","file_name","status","record_count_hint","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);
  recon="";
  file="";
  status="";
  record="";

  if (lower ~ /kafka message/) {
    event="KAFKA_MESSAGE";
  }
  else if (lower ~ /audit/) {
    event="AUDIT";
  }
  else if (lower ~ /insert/) {
    event="AUDIT_INSERT";
  }
  else {
    event="OTHER_AUDIT";
  }

  if (match(raw, /"file_name":"[^"]+"/)) {
    file=substr(raw, RSTART+13, RLENGTH-14);
  }

  if (match(raw, /"recon_id":"[^"]+"/)) {
    recon=substr(raw, RSTART+12, RLENGTH-13);
  }
  else if (match(raw, /recon_id[=: ]+[0-9]+/)) {
    recon=substr(raw, RSTART, RLENGTH);
  }

  if (lower ~ /success/) {
    status="SUCCESS";
  }
  else if (lower ~ /failed|failure|error/) {
    status="FAILED_OR_ERROR";
  }

  if (match(raw, /record_count[=: ]+[0-9]+/)) {
    record=substr(raw, RSTART, RLENGTH);
  }
  else if (match(raw, /"record_count":"[^"]+"/)) {
    record=substr(raw, RSTART+16, RLENGTH-17);
  }

  gsub(/\|/, " ", raw);
  print ts,thread,event,recon,file,status,record,raw;
}
' > 07_kafka_audit_events.psv

echo "Created 07_kafka_audit_events.psv"


# =============================================================================
# 08. File size / record count possible events
# =============================================================================

grep -iE "file size|filesize|size=|bytes|length|content length|len=|record_count|record count|records|number of records|input size|output size" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","event_type","file_or_metric_hint","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);
  hint="";

  if (lower ~ /file size|filesize|bytes|length|content length|len=/) {
    event="FILE_SIZE_OR_BYTES";
  }
  else if (lower ~ /record_count|record count|records|number of records/) {
    event="RECORD_COUNT";
  }
  else if (lower ~ /input size/) {
    event="INPUT_SIZE";
  }
  else if (lower ~ /output size/) {
    event="OUTPUT_SIZE";
  }
  else {
    event="SIZE_RECORD_OTHER";
  }

  if (match(raw, /[A-Za-z0-9._-]+\.dat\.gz/)) {
    hint=substr(raw, RSTART, RLENGTH);
  }
  else if (match(raw, /[A-Za-z0-9._-]+\.csv/)) {
    hint=substr(raw, RSTART, RLENGTH);
  }
  else if (match(raw, /[A-Za-z0-9._-]+\.txt/)) {
    hint=substr(raw, RSTART, RLENGTH);
  }

  gsub(/\|/, " ", raw);
  print ts,thread,event,hint,raw;
}
' > 08_file_size_record_count_events.psv

echo "Created 08_file_size_record_count_events.psv"


# =============================================================================
# 09. Thread summary
# =============================================================================

grep -i "processing file name" "$LOG_FILE" \
| awk '{print $2}' \
| sort \
| uniq -c \
| awk 'BEGIN{OFS="|"; print "processing_event_count","thread"} {print $1,$2}' \
> 09_thread_summary.psv

echo "Created 09_thread_summary.psv"


# =============================================================================
# 10. All relevant events combined
# =============================================================================

grep -iE "processing file name|processFile|recon_id|reconid|recon name|file size|filesize|bytes|record_count|record count|records|transformation|transform|dmw|filter|filtercriteria|aggregation|generatedfeed|xml|parquet|pqt|write|save|output path|target path|InsertIntoHadoopFsRelation|FileOutputCommitter|commit|archive|moving file|kafka message|audit|shuffle|spill|GC|executor|DAGScheduler|Job|Stage|Exception|ERROR|FAILED|SUCCESS" "$LOG_FILE" \
| awk '
BEGIN {
  OFS="|";
  print "timestamp","thread","category","raw_log";
}
{
  ts=$1;
  thread=$2;
  raw=$0;
  lower=tolower(raw);

  if (lower ~ /processing file name|processfile/) {
    cat="FILE_PROCESSING";
  }
  else if (lower ~ /transformation|transform|dmw|filter|filtercriteria|aggregation|generatedfeed|xml/) {
    cat="TRANSFORMATION_FILTER";
  }
  else if (lower ~ /parquet|pqt|write|save|commit/) {
    cat="PARQUET_WRITE";
  }
  else if (lower ~ /dagscheduler|job|stage|sparkcontext|executor|task|shuffle|spill|gc/) {
    cat="SPARK_EVENT";
  }
  else if (lower ~ /archive|moving file|move|rename|delete|copy/) {
    cat="HDFS_ARCHIVE_MOVE";
  }
  else if (lower ~ /kafka|audit|insert/) {
    cat="KAFKA_AUDIT";
  }
  else if (lower ~ /file size|filesize|bytes|record_count|record count|records/) {
    cat="SIZE_RECORD";
  }
  else if (lower ~ /exception|error|failed/) {
    cat="ERROR_FAILURE";
  }
  else {
    cat="OTHER_RELEVANT";
  }

  gsub(/\|/, " ", raw);
  print ts,thread,cat,raw;
}
' > 10_all_relevant_events.psv

echo "Created 10_all_relevant_events.psv"


echo ""
echo "Done."
echo "Generated files:"
echo "  01_file_processing_events.psv"
echo "  02_file_lifecycle_summary.psv"
echo "  03_transformation_filter_events.psv"
echo "  04_parquet_write_events.psv"
echo "  05_spark_job_stage_events.psv"
echo "  06_archive_move_events.psv"
echo "  07_kafka_audit_events.psv"
echo "  08_file_size_record_count_events.psv"
echo "  09_thread_summary.psv"
echo "  10_all_relevant_events.psv"
