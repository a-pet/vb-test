#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: $0 <prospect_name> <customer_id> <dbt_command>"
  exit 1
fi

PROSPECT_NAME="$1"
CUSTOMER_ID="$2"
shift 2

docker-compose -f docker-compose.yml -f docker-compose.override.yml run --rm -e PROSPECT_NAME="$PROSPECT_NAME" -e CUSTOMER_ID="$CUSTOMER_ID" dbt "$@"
read -p "Press [Enter] key to exit..."
