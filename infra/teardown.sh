#!/bin/bash
# ============================================================
# teardown.sh — Delete ALL resources to stop Azure billing
# Usage: ./teardown.sh
# WARNING: This permanently deletes everything in the RG
# ============================================================

set -e

# ── Config ────────────────────────────────────────────────
RESOURCE_GROUP="rg-nyc-taxi-pipeline"
SUFFIX="sri01"
MANAGED_RG="rg-dbw-managed-$SUFFIX"   # Dynamic (fix)

# ── Colours ───────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}===================================================${NC}"
echo -e "${RED}  ⚠  TEARDOWN — This will DELETE all resources    ${NC}"
echo -e "${RED}===================================================${NC}"
echo ""
echo -e "Resource group to delete: ${YELLOW}$RESOURCE_GROUP${NC}"
echo ""
echo -e "${RED}All ADF pipelines, ADLS data, SQL DB, Databricks${NC}"
echo -e "${RED}workspace will be permanently deleted.${NC}"
echo ""

# ── Confirmation ───────────────────────────────────────────
read -p "Type 'yes-delete-all' to confirm: " CONFIRM

if [ "$CONFIRM" != "yes-delete-all" ]; then
  echo "Teardown cancelled."
  exit 0
fi

# ── Step 1: Delete main RG ─────────────────────────────────
echo -e "\n${GREEN}[1/3] Deleting main resource group...${NC}"

if az group exists --name "$RESOURCE_GROUP"; then
  az group delete \
    --name "$RESOURCE_GROUP" \
    --yes \
    --no-wait
else
  echo "Resource group not found."
fi

# ── Step 2: Delete Databricks managed RG ───────────────────
echo -e "\n${GREEN}[2/3] Deleting Databricks managed resource group...${NC}"

if az group exists --name "$MANAGED_RG"; then
  az group delete \
    --name "$MANAGED_RG" \
    --yes \
    --no-wait
else
  echo "Managed RG not found or already deleted."
fi

# ── Step 3: Soft-delete cleanup ────────────────────────────
echo -e "\n${GREEN}[3/3] Checking for soft-deleted Data Factory instances...${NC}"
az datafactory list-deleted 2>/dev/null || true

# ── Final Message ──────────────────────────────────────────
echo -e "\n${YELLOW}⏳ Deletion running in background (takes ~3-5 min)${NC}"
echo -e "${GREEN}✅ Teardown initiated. Monitor at portal.azure.com${NC}"
echo ""
echo -e "${YELLOW}Your code is safe in GitHub. To redeploy anytime:${NC}"
echo "  cd infra"
echo "  bash deploy.sh"