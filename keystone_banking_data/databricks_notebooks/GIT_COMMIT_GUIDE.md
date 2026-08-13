# Git Commit Guide - Keystone Banking Project

## Understanding the Git Workflow

### Current Situation

You have Databricks notebooks and files in:
```
/Workspace/Users/vincentchitsike2002.vc@gmail.com/Data-Project-End-to-End/keystone_banking_data/databricks_notebooks/
```

You want to commit these to GitHub at:
```
https://github.com/inhamo/Datasets-Advanced-2026.git
```

### Important Concepts

#### 1. **Databricks Notebooks vs Git Files**

- **Databricks stores notebooks** in its own format (.ipynb internally)
- **Git needs files** that can be tracked (text files, scripts, etc.)
- You have two options:
  - Export notebooks as `.py` or `.ipynb` files
  - Use Databricks Git integration (Repos)

#### 2. **What to Commit**

✅ **Safe to commit:**
- `000_config` notebook (cleaned - no secrets)
- `README.md` (documentation)
- `setup_secrets.sh` (setup script)
- `MIGRATION_SUMMARY_2026-08-12.md` (migration guide)
- `.gitignore` (to exclude sensitive files)

❌ **Never commit:**
- Actual secret values
- Personal access tokens
- Real usernames/repo names (use placeholders)
- API keys or credentials

---

## Method 1: Using Databricks Git Integration (Recommended)

### Step 1: Create a Databricks Repo

**In Databricks UI:**
1. Go to **Workspace** → **Repos**
2. Click **Add Repo**
3. Enter:
   - **Git repository URL**: `https://github.com/inhamo/Datasets-Advanced-2026.git`
   - **Git provider**: GitHub
   - **Repository name**: `Datasets-Advanced-2026`
4. Authenticate with GitHub (personal access token or OAuth)

### Step 2: Move/Copy Files to Repo

```bash
# In Databricks terminal
cd /Workspace/Repos/vincentchitsike2002.vc@gmail.com/Datasets-Advanced-2026/

# Copy your cleaned files
cp /Workspace/Users/vincentchitsike2002.vc@gmail.com/Data-Project-End-to-End/keystone_banking_data/databricks_notebooks/000_config .
cp /Workspace/Users/vincentchitsike2002.vc@gmail.com/Data-Project-End-to-End/keystone_banking_data/databricks_notebooks/README.md .
cp /Workspace/Users/vincentchitsike2002.vc@gmail.com/Data-Project-End-to-End/keystone_banking_data/databricks_notebooks/setup_secrets.sh .
```

### Step 3: Commit Using Databricks UI

1. In the Repo folder, click **Git**
2. Review changed files
3. Enter commit message: "Add cleaned configuration and setup files"
4. Click **Commit & Push**

---

## Method 2: Manual Git from Terminal

### Prerequisites

**You need:**
- Git configured in your workspace
- GitHub personal access token (PAT)

**Create GitHub PAT:**
1. Go to GitHub → Settings → Developer settings → Personal access tokens
2. Generate new token (classic)
3. Select scopes: `repo` (full control)
4. Copy the token (you'll need it for authentication)

### Step 1: Initialize Git Repository

```bash
cd /Workspace/Users/vincentchitsike2002.vc@gmail.com/Data-Project-End-to-End/keystone_banking_data/databricks_notebooks

# Initialize git
git init

# Configure your identity
git config user.email "vincentchitsike2002.vc@gmail.com"
git config user.name "Vincent Chitsike"
```

### Step 2: Create .gitignore

```bash
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python

# Jupyter Notebook
.ipynb_checkpoints

# Environment
.env
.venv
env/
venv/

# Secrets (safety net)
*secret*
*password*
*token*
.databrickscfg

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db
EOF
```

### Step 3: Export Notebooks to .py Format

**Option A: Using Databricks CLI**
```bash
# Export notebook to Python file
databricks workspace export /Users/vincentchitsike2002.vc@gmail.com/Data-Project-End-to-End/keystone_banking_data/databricks_notebooks/000_config \
  ./000_config.py \
  --format SOURCE
```

**Option B: Using workspace UI**
1. Open the notebook
2. File → Export → Source File (.py)
3. Save to your local machine
4. Upload back to the terminal location

### Step 4: Stage Files

```bash
# Add specific files
git add README.md
git add setup_secrets.sh
git add MIGRATION_SUMMARY_2026-08-12.md
git add .gitignore

# If you exported the notebook as .py
git add 000_config.py

# Check what will be committed
git status
```

### Step 5: Commit

```bash
git commit -m "Initial commit: Add cleaned configuration and setup files

- Add 000_config notebook (all secrets externalized to Databricks Secrets)
- Add README with setup instructions
- Add setup_secrets.sh for automated secret configuration
- Add migration summary documentation
- No hardcoded credentials or sensitive information"
```

### Step 6: Connect to Remote

```bash
# Add remote repository
git remote add origin https://github.com/inhamo/Datasets-Advanced-2026.git

# Verify remote
git remote -v
```

### Step 7: Push to GitHub

**You'll need your GitHub Personal Access Token here:**

```bash
# Push to main branch
git push -u origin main

# If your default branch is 'master'
# git push -u origin master

# You'll be prompted for credentials:
# Username: inhamo
# Password: <paste your GitHub Personal Access Token>
```

**Alternative - Use Token in URL (less secure):**
```bash
git remote set-url origin https://<YOUR_GITHUB_TOKEN>@github.com/inhamo/Datasets-Advanced-2026.git
git push -u origin main
```

---

## Method 3: Using Databricks CLI (Recommended for Automation)

### Step 1: Install and Configure Databricks CLI

```bash
# Already installed in your workspace
# Configure if not already done
databricks configure --token
```

### Step 2: Use Git Commands Through Databricks

```bash
# The databricks CLI has git integration
databricks repos create \
  --url https://github.com/inhamo/Datasets-Advanced-2026.git \
  --provider gitHub \
  --path /Repos/vincentchitsike2002.vc@gmail.com/Datasets-Advanced-2026
```

---

## Pre-Commit Security Checklist

✅ Before you push, verify:

```bash
# Search for potential secrets in files you're committing
grep -r "password" .
grep -r "token" .
grep -r "key" .
grep -r "secret" .

# Check that config uses dbutils.secrets.get()
grep -r "dbutils.secrets.get" 000_config*

# Ensure no hardcoded values
grep -r "keystone_2026" .  # Should return nothing
grep -r "inhamo" .  # Should only be in documentation/examples
```

✅ **All checks should pass before pushing!**

---

## After First Commit

### Daily Workflow

```bash
# Pull latest changes
git pull origin main

# Make your changes...

# Check what changed
git status
git diff

# Stage and commit
git add <files>
git commit -m "Your descriptive message"

# Push to GitHub
git push origin main
```

### Working with Branches

```bash
# Create a feature branch
git checkout -b feature/add-bronze-ingestion

# Work on your feature...

# Commit changes
git add .
git commit -m "Add bronze layer ingestion notebook"

# Push branch
git push origin feature/add-bronze-ingestion

# Create pull request on GitHub
# After approval, merge and delete branch
```

---

## Troubleshooting

### Issue: "Authentication failed"

**Solution**: Use GitHub Personal Access Token
```bash
# Generate token at: https://github.com/settings/tokens
# Use token as password when prompted
```

### Issue: "Remote already exists"

**Solution**: Remove and re-add
```bash
git remote remove origin
git remote add origin https://github.com/inhamo/Datasets-Advanced-2026.git
```

### Issue: "Nothing to commit"

**Solution**: Check your changes
```bash
git status
ls -la  # Verify files exist
```

### Issue: "Permission denied"

**Solution**: Check repository access
- Verify you have write access to the GitHub repo
- Verify your PAT has `repo` scope

---

## Best Practices

### 1. **Commit Messages**

Good:
```
✅ "Add customer PII masking in bronze layer"
✅ "Fix: Handle null values in transaction amount"
✅ "Refactor: Extract config loading to utility function"
```

Bad:
```
❌ "update"
❌ "fix bug"
❌ "changes"
```

### 2. **Commit Frequency**

- Commit after completing a logical unit of work
- Don't wait days/weeks to commit
- Commit before major refactoring

### 3. **Never Commit**

❌ Secrets, tokens, passwords
❌ Large data files (use Git LFS or exclude)
❌ Temporary/debug code
❌ Personal configuration files

### 4. **Always Review Before Push**

```bash
# Review what you're about to push
git diff origin/main..HEAD

# Check for secrets one more time
git diff origin/main..HEAD | grep -i "password\|secret\|token"
```

---

## Next Steps

Once you understand this workflow, I can:
1. Help you choose the best method for your setup
2. Execute the commands for you
3. Verify the commit is safe before pushing
4. Set up branch protection and CI/CD

---

**Ready to commit?** Tell me which method you prefer!