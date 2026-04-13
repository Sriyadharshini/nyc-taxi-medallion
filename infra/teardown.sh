#!/bin/bash
# ============================================================
# teardown.sh — Delete ALL resources (reliable version)
# ============================================================

set -e

# ── Config ────────────────────────────────────────────────
RESOURCE_GROUP="rg-nyc-taxi-pipeline"
SUFFIX="sri01"
MANAGED_RG="rg-dbw-managed-$SUFFIX"

# ── Colors ────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}===================================================${NC}"
echo -e "${RED}  ⚠  TEARDOWN — This will DELETE all resources    ${NC}"
echo -e "${RED}===================================================${NC}"
echo ""
echo "Resource group: $RESOURCE_GROUP"
echo ""

# ── Confirmation ───────────────────────────────────────────
read -p "Type 'yes-delete-all' to confirm: " CONFIRM
if [ "$CONFIRM" != "yes-delete-all" ]; then
  echo "Teardown cancelled."
  exit 0
fi

# ── Step 1: Delete main RG (WAIT for completion) ───────────
echo -e "\n${GREEN}[1/2] Deleting main resource group...${NC}"

if az group exists --name "$RESOURCE_GROUP"; then
  az group delete \
    --name "$RESOURCE_GROUP" \
    --yes \
    --no-wait

  echo "Waiting for main RG deletion..."

  while az group exists --name "$RESOURCE_GROUP"; do
    echo "Still deleting..."
    sleep 10
  done

  echo "Main RG deleted ✅"
else
  echo "Main RG not found."
fi

# ── Step 2: Delete Databricks managed RG ───────────────────
echo -e "\n${GREEN}[2/2] Deleting Databricks managed RG...${NC}"

if az group exists --name "$MANAGED_RG"; then
  az group delete \
    --name "$MANAGED_RG" \
    --yes \
    --no-wait

  echo "Waiting for managed RG deletion..."

  while az group exists --name "$MANAGED_RG"; do
    echo "Still deleting managed RG..."
    sleep 10
  done

  echo "Managed RG deleted ✅"
else
  echo "Managed RG not found or already deleted."
fi

# ── Final ────────────────────────────────────────────────
echo ""
echo -e "${GREEN}✅ All resources deleted successfully!${NC}"
echo -e "${YELLOW}You can redeploy anytime using:${NC}"
echo "cd infra && bash deploy.sh"