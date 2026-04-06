#!/bin/bash
# ============================================================
# deploy.sh — Spin up full NYC Taxi pipeline infrastructure
# Usage: ./deploy.sh
# ============================================================

set -e  # Exit immediately on any error

# ── Config ───────────────────────────────────────────────────
RESOURCE_GROUP="rg-nyc-taxi-pipeline"
LOCATION="eastus"
TEMPLATE="./main.bicep"
PARAMS="./parameters.json"
SQL_PASSWORD="${SQL_ADMIN_PASSWORD}"   # Set this as env variable before running

# ── Colour output ─────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}===================================================${NC}"
echo -e "${YELLOW}  NYC Taxi Medallion — Infrastructure Deploy       ${NC}"
echo -e "${YELLOW}===================================================${NC}"

# ── Step 1: Login check ───────────────────────────────────────
echo -e "\n${GREEN}[1/5] Checking Azure login...${NC}"
az account show > /dev/null 2>&1 || {
  echo "Not logged in. Running az login..."
  az login
}

SUBSCRIPTION=$(az account show --query name -o tsv)
echo "Subscription: $SUBSCRIPTION"

# ── Step 2: Create Resource Group ────────────────────────────
echo -e "\n${GREEN}[2/5] Creating resource group...${NC}"
az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --tags project=nyc-taxi-medallion managedBy=bicep owner=sri

# ── Step 3: Validate template ─────────────────────────────────
echo -e "\n${GREEN}[3/5] Validating Bicep template...${NC}"
az deployment group validate \
  --resource-group "$RESOURCE_GROUP" \
  --template-file "$TEMPLATE" \
  --parameters "@$PARAMS" \
 

# ── Step 4: Deploy ────────────────────────────────────────────
echo -e "\n${GREEN}[4/5] Deploying infrastructure (this takes ~5 min)...${NC}"
az deployment group create \
  --resource-group "$RESOURCE_GROUP" \
  --template-file "$TEMPLATE" \
  --parameters "@$PARAMS" \
  --name "deploy-$(date +%Y%m%d-%H%M%S)" \
  --output table

# ── Step 5: Print outputs ─────────────────────────────────────
echo -e "\n${GREEN}[5/5] Deployment complete! Resource details:${NC}"
az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name $(az deployment group list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv) \
  --query properties.outputs \
  --output table

echo -e "\n${GREEN}✅ All resources created successfully!${NC}"
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Install Self-Hosted Integration Runtime on your laptop"
echo "  2. Configure linked services in ADF Studio"
echo "  3. Run watermark SQL setup: cd ../sql && run watermark_setup.sql"
echo ""
echo -e "${RED}⚠  Remember: Delete resources when done to save credits!${NC}"
echo "   Run: ./teardown.sh"
