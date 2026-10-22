#!/usr/bin/env bash
# Run the Scala vendor summary job in spark-shell.
#   bash spark/scala/run_scala.sh [INPUT_DIR] [OUTPUT_DIR]
# Needs spark-shell on PATH (it ships with `pip install pyspark`: $(python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")/bin).
set -euo pipefail
INPUT_DIR="${1:-spark/sample_data}"
OUTPUT_DIR="${2:-spark/output_scala}"
SHELL_BIN="$(command -v spark-shell || echo "$(python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')/bin/spark-shell")"
printf ':paste spark/scala/VendorSummary.scala\n' | \
  INPUT_DIR="$INPUT_DIR" OUTPUT_DIR="$OUTPUT_DIR" "$SHELL_BIN" --conf spark.sql.shuffle.partitions=8
test -d "$OUTPUT_DIR/vendor_sales_summary_parquet" || { echo "Scala job did not write output"; exit 1; }
