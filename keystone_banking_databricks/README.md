# Keystone Banking - Databricks Notebooks

Converted Microsoft Fabric notebooks for the Keystone Banking end-to-end data engineering project.

## 🚀 Quick Start

### Prerequisites

1. **Databricks CLI installed:**
   ```bash
   pip install databricks-cli
   ```

2. **Databricks CLI configured:**
   ```bash
   databricks configure --token
   ```
   You'll need:
   - Databricks workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
   - Personal access token (generate from User Settings → Access Tokens)

### Step 1: Set Up Secrets

**Option A: Automated Setup (Recommended)**

```bash
chmod +x setup_secrets.sh
./setup_secrets.sh
```

The script will:
* Create the `keystone_banking` secret scope
* Prompt you for each required secret value
* Verify the setup

**Option B: Manual Setup**

```bash
# Create scope
databricks secrets create-scope keystone_banking

# Add secrets (you'll be prompted for values)
databricks secrets put --scope keystone_banking --key github_host
databricks secrets put --scope keystone_banking --key github_user
databricks secrets put --scope keystone_banking --key github_repo
databricks secrets put --scope keystone_banking --key mask_salt
```

**Required Secret Values:**

| Key | Description | Example |
|-----|-------------|----------|
| `github_host` | GitHub base URL | `https://github.com` |
| `github_user` | Your GitHub username | `your-username` |
| `github_repo` | Repository name | `your-repo-name` |
| `mask_salt` | PII masking salt (keep secret!) | `your-secure-random-value` |

### Step 2: Verify Setup

Run the [000_config](#notebook-454464539297736) notebook to test the configuration:

1. Execute Cell 1 (Configuration Setup)
2. If successful, you'll see: ✅ Successfully loaded all secrets
3. Optionally run Cell 3 to test the full configuration

### Step 3: Set Up Unity Catalog

```sql
CREATE CATALOG IF NOT EXISTS keystone_banking;

CREATE SCHEMA IF NOT EXISTS keystone_banking.bronze;
CREATE SCHEMA IF NOT EXISTS keystone_banking.silver;
CREATE SCHEMA IF NOT EXISTS keystone_banking.gold;
CREATE SCHEMA IF NOT EXISTS keystone_banking.control;
```

## 📁 Project Structure

```
databricks_notebooks/
├── README.md                          # This file
├── setup_secrets.sh                   # Automated secrets setup
├── MIGRATION_SUMMARY_2026-08-12.md   # Detailed migration guide
├── 000_config                         # ✅ Configuration notebook (CONVERTED)
└── [37 notebooks pending conversion]
```

## 📊 Notebook Inventory

### ✅ Converted (1)
- **000_config** - Configuration with secrets management

### 🔄 Pending Conversion (37)

See [MIGRATION_SUMMARY_2026-08-12.md](#file-454464539297737) for:
- Full list of notebooks to convert
- Detailed conversion patterns
- Step-by-step migration guide

## 🔒 Security Best Practices

### ✅ DO:
* Store all secrets in Databricks Secrets
* Use unique salt values for each environment
* Rotate secrets regularly
* Grant secret scope access only to authorized users
* Use separate scopes for dev/staging/prod

### ❌ DON'T:
* Commit secrets to Git
* Hardcode credentials in notebooks
* Share secrets via email/chat
* Print or log secret values
* Use production secrets in development

## 🛠️ Common Commands

### List Secrets
```bash
databricks secrets list-secrets keystone_banking
```

### Update a Secret
```bash
databricks secrets put --scope keystone_banking --key mask_salt
```

### Delete a Secret
```bash
databricks secrets delete --scope keystone_banking --key secret_name
```

### View Secret Scopes
```bash
databricks secrets list-scopes
```

## 📚 Resources

* [Databricks Secrets Documentation](https://docs.databricks.com/security/secrets/index.html)
* [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
* [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
* [Migration Summary](./MIGRATION_SUMMARY_2026-08-12.md)

## 🐛 Troubleshooting

### Issue: "Secret scope 'keystone_banking' does not exist"
**Solution:** Run `./setup_secrets.sh` or manually create the scope

### Issue: "Secret 'xyz' not found in scope"
**Solution:** Add the missing secret using `databricks secrets put`

### Issue: "Table or view not found"
**Solution:** Ensure Unity Catalog structure is created (see Step 3 above)

### Issue: "Permission denied"
**Solution:** Contact workspace admin to grant access to the secret scope

## 📞 Support

For questions about:
- **Migration:** See [MIGRATION_SUMMARY_2026-08-12.md](#file-454464539297737)
- **Secrets:** See [000_config](#notebook-454464539297736) cell 2
- **General:** Reach out to your Databricks administrator

---

**Last Updated:** August 12, 2026  
**Status:** Initial setup complete, 37 notebooks pending conversion