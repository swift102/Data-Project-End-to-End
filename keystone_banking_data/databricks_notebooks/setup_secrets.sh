#!/bin/bash

# Databricks Secrets Setup Script for Keystone Banking Project
# This script creates the secret scope and prompts for all required secrets

set -e  # Exit on error

SCOPE_NAME="keystone_banking"

echo "========================================"
echo "Keystone Banking - Secrets Setup"
echo "========================================"
echo ""

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "❌ Error: Databricks CLI is not installed"
    echo ""
    echo "Install it with:"
    echo "  pip install databricks-cli"
    echo ""
    echo "Then configure authentication:"
    echo "  databricks configure --token"
    exit 1
fi

echo "Step 1: Creating secret scope '$SCOPE_NAME'"
echo "-------------------------------------------"

# Try to create the scope (will fail if it already exists, which is fine)
if databricks secrets create-scope "$SCOPE_NAME" 2>/dev/null; then
    echo "✅ Secret scope '$SCOPE_NAME' created successfully"
else
    echo "ℹ️  Secret scope '$SCOPE_NAME' already exists (skipping creation)"
fi

echo ""
echo "Step 2: Adding secrets to the scope"
echo "-------------------------------------------"
echo ""

# Helper function to add a secret
add_secret() {
    local key=$1
    local description=$2
    local default_value=$3
    
    echo "Setting: $key"
    echo "Description: $description"
    
    if [ -n "$default_value" ]; then
        echo "Default value: $default_value"
        read -p "Press Enter to use default, or type a new value: " user_value
        value=${user_value:-$default_value}
    else
        read -p "Enter value: " value
    fi
    
    # Use the correct CLI syntax (positional arguments)
    databricks secrets put-secret "$SCOPE_NAME" "$key" --string-value "$value"
    echo "✅ Secret '$key' added successfully"
    echo ""
}

# Add each required secret
echo "1/4: GitHub Host"
add_secret "github_host" "GitHub base URL" "https://github.com"

echo "2/4: GitHub User"
add_secret "github_user" "Your GitHub username" ""

echo "3/4: GitHub Repository"
add_secret "github_repo" "Repository name" ""

echo "4/4: Data Masking Salt"
echo "⚠️  IMPORTANT: Use a strong, unique value for production!"
add_secret "mask_salt" "Salt value for PII masking" ""

echo ""
echo "========================================"
echo "✅ Setup Complete!"
echo "========================================"
echo ""
echo "Secrets added to scope '$SCOPE_NAME':"
echo "  ✓ github_host"
echo "  ✓ github_user"
echo "  ✓ github_repo"
echo "  ✓ mask_salt"
echo ""
echo "To verify, run:"
echo "  databricks secrets list --scope $SCOPE_NAME"
echo ""
echo "You can now run the 000_config notebook!"
echo ""