#!/bin/bash

# =================================================================================
# Function to delete S3 objects older than a specified number of days, with parallel processing.
#
# Arguments:
#   $1 - The full S3 path (e.g., "s3://my-bucket/path/").
#   $2 - The number of days to keep. Objects older than this will be deleted.
#   $3 - Optional. Use "--delete" to perform the actual deletion.
#   $4 - Optional. Number of parallel processes (default is 10).
#
# Example Usage:
#   # Dry Run (recommended)
#   delete_old_s3_objects "s3://your-bucket-name/path/to/savepoints/" 7
#
#   # Actual Deletion with 20 parallel processes
#   delete_old_s3_objects "s3://your-bucket-name/path/to/savepoints/" 7 --delete 20
# =================================================================================

delete_old_s3_objects() {
  local S3_PATH="$1"
  local DAYS_TO_KEEP="$2"
  local ACTION="$3"
  local PARALLEL_PROCESSES="${4:-10}" # Default to 10 if not provided

  if [[ -z "$S3_PATH" || -z "$DAYS_TO_KEEP" ]]; then
    echo "Error: Both S3_PATH and DAYS_TO_KEEP must be provided."
    echo "Usage: delete_old_s3_objects \"s3://path\" 7 [--delete] [num_threads]"
    return 1
  fi

  echo "Processing S3 path: $S3_PATH"
  echo "Retention policy: Deleting objects older than $DAYS_TO_KEEP days."
  echo "Number of parallel processes: $PARALLEL_PROCESSES"
  echo "------------------------------------------------------------------"

  # === FIX FOR 'date: illegal option -- d' ERROR ===
  # This line uses the BSD/macOS 'date' syntax for calculating the cutoff date.
  # If you were on a GNU/Linux system, you would use:
  # local CUTOFF_DATE=$(date -d "$DAYS_TO_KEEP days ago" "+%Y-%m-%d")
  local CUTOFF_DATE=$(date -v-"$DAYS_TO_KEEP"d "+%Y-%m-%d")
  echo "Cutoff date (YYYY-MM-DD): $CUTOFF_DATE"

  # Extract bucket name from S3_PATH for proper object deletion
  local BUCKET_NAME=$(echo "$S3_PATH" | sed 's|s3://||' | cut -d'/' -f1)
  local PREFIX=$(echo "$S3_PATH" | sed 's|s3://[^/]*/||')

  # Dry run or deletion
  if [[ "$ACTION" == "--delete" ]]; then
    echo "Performing ACTUAL DELETION. Objects being removed:"

    # Identify objects and pipe their full S3 path to xargs for parallel deletion
    aws s3 ls "$S3_PATH" --recursive | while read -r line; do
      local OBJECT_DATE=$(echo "$line" | awk '{print $1}')
      local OBJECT_KEY=$(echo "$line" | awk '{$1=""; $2=""; $3=""; print substr($0,4)}')
      if [[ "$OBJECT_DATE" < "$CUTOFF_DATE" ]]; then
        #echo "  -> Deleting (Date: $OBJECT_DATE): s3://$BUCKET_NAME/$OBJECT_KEY"
        echo "s3://$BUCKET_NAME/$OBJECT_KEY"
      fi
    done | xargs -n 1 -P "$PARALLEL_PROCESSES" aws s3 rm

  else
    echo "Performing DRY RUN. The following objects WOULD be deleted:"

    # Identify objects and print them
    aws s3 ls "$S3_PATH" --recursive | while read -r line; do
      local OBJECT_DATE=$(echo "$line" | awk '{print $1}')
      local OBJECT_KEY=$(echo "$line" | awk '{$1=""; $2=""; $3=""; print substr($0,4)}')
      if [[ "$OBJECT_DATE" < "$CUTOFF_DATE" ]]; then
        echo "  -> Found old object (Date: $OBJECT_DATE): s3://$BUCKET_NAME/$OBJECT_KEY"
      fi
    done
  fi

  if [[ "$ACTION" == "--delete" ]]; then
    echo "------------------------------------------------------------------"
    echo "Parallel deletion complete."
  else
    echo "------------------------------------------------------------------"
    echo "Dry run complete. No objects were deleted."
    echo "To run the actual deletion, call the function with '--delete' as the third argument."
  fi
}
delete_old_s3_objects $1 $2 $3 $4