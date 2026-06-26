# Customer 360 View — Individual Customers
## `vw_customer_360_individual`

**Layer:** Gold (T-SQL View — SQL Analytics Endpoint)
**Source tables:** `lh_silver_banking_data`
**Scope:** Individual customers only (`customers_individual`)
**Purpose:** Customer segmentation, behavioural analytics, and CRM enrichment

---

## Column Specification

### 1. Identity & Demographics

| Column | Source | Type | Notes |
|---|---|---|---|
| `customer_id` | `customers_individual` | STRING | Natural key |
| `birth_date` | `customers_individual` | DATE | Corrected in Silver (dq_birth_date_corrected) |
| `city` | `customers_individual.branch_city` | STRING | |
| `province` | `customers_individual.branch_province` | STRING | |
| `nationality` | `customers_individual` | STRING | |
| `citizenship` | `customers_individual` | STRING | |
| `risk_score` | `customers_individual` | FLOAT | 0–1 scale |
| `preferred_contact_method` | `customers_individual` | STRING | |
| `is_pep` | `customers_individual` | BIT | Politically Exposed Person flag |
| `expected_income` | `customers_individual.annual_income` | FLOAT | Self-declared at onboarding |
| `is_opted_in_marketing` | `marketing_campaign_responses` | BIT | `1` = positive engagement + no opt-out, `0` = opted out/complained, `NULL` = no campaign record |

---

### 2. Account Metrics

| Column | Source | Type | Notes |
|---|---|---|---|
| `number_of_accounts` | `bridge_customer_account` + `accounts` | INT | Primary Holder + Joint Holder only |
| `first_account_opening_date` | `accounts.opening_date` | DATE | Earliest account across all owned accounts |
| `age_at_opening` | Derived | INT | `FLOOR(DATEDIFF(DAY, birth_date, first_account_opening_date) / 365.25)` — historical fact, stable across query runs |
| `has_overdraft` | `accounts.has_overdraft` | BIT | 1 if any owned account has overdraft facility |
| `has_credit_card` | `accounts.has_credit_card` | BIT | 1 if any owned account has credit card |

> **Analyst-derived columns (not in view — dynamic by query date):**
> - `customer_age` → `FLOOR(DATEDIFF(DAY, birth_date, GETDATE()) / 365.25)`
> - `customer_tenure_years` → `FLOOR(DATEDIFF(DAY, first_account_opening_date, GETDATE()) / 365.25)`

---

### 3. Transaction & Behavioural Metrics

| Column | Source | Type | Attribution | Notes |
|---|---|---|---|---|
| `transactions_frequency` | `transactions` | FLOAT | Primary + Joint | Transactions per active month |
| `number_of_active_months` | `transactions` | INT | Primary + Joint | Distinct `yyyy-MM` periods with at least one transaction |
| `last_day_of_transactions` | `transactions` | DATE | Primary + Joint | Most recent transaction date |
| `average_days_between_transactions` | `transactions` | FLOAT | Primary + Joint | Average gap in days between consecutive transactions |
| `average_transaction_volume` | `transactions` | FLOAT | **Primary only** | `AVG(ABS(amount))` — Primary Holder only to avoid double-counting on joint accounts |
| `average_balance` | `transactions` | FLOAT | **Primary only** | Running balance per account (`cumulative SUM(amount)`) averaged across all dates — pre-aggregated in `fact_transaction`, not computed at view runtime |
| `most_preferred_channel` | `transactions` | STRING | Primary + Joint | Channel with highest transaction count |

> **Attribution rule:**
> - **Engagement metrics** (frequency, active months, last date, days between, preferred channel) — Primary Holder + Joint Holder. Both relationships represent real activity for the customer.
> - **Financial metrics** (transaction volume, balance) — Primary Holder only. Joint account amounts must not be counted for every joint holder or total money movement is inflated.

---

### 4. Loan Metrics

| Column | Source | Type | Notes |
|---|---|---|---|
| `has_had_a_loan` | `loans` | BIT | 1 if customer has any loan record |
| `number_of_loans` | `loans` | INT | Count of distinct `loan_id` per customer |

---

### 5. Salary Indicators

| Column | Derived From | Type | Logic |
|---|---|---|---|
| `is_on_regular_salary` | `transactions.is_salary_candidate` | BIT | 1 if `salary_transaction_count >= 3` |
| `has_salary_stopped` | `transactions.is_salary_candidate` | BIT | 1 if salary count ≥ 3 AND `last_salary_date < DATEADD(MONTH, -1, last_day_of_transactions)` |

> **Known limitation:** `is_salary_candidate = 0` for all rows in current dataset — the salary inflow pattern is not detectable against this dataset's transaction generator logic. Both flags will return `0` universally until source data changes.

---

### 6. Quartile Rankings

All quartiles use `NTILE(4)` across the full customer population. `1` = lowest quartile, `4` = highest.

| Column | Based On |
|---|---|
| `transactions_frequency_quartile` | `transactions_frequency` |
| `active_months_quartile` | `number_of_active_months` |
| `avg_days_between_transactions_quartile` | `average_days_between_transactions` |
| `avg_transaction_volume_quartile` | `average_transaction_volume` |
| `avg_balance_quartile` | `average_balance` *(pending `fact_transaction`)* |

---

## Implementation Notes

### `average_balance` — pending `fact_transaction`
Running balance cannot be computed efficiently at view query time across 5M+ transaction rows.
The correct pattern:
1. `fact_transaction` pre-computes `running_balance` per account per transaction using a window function during the Gold build
2. `average_balance` per customer is then `AVG(running_balance)` grouped by `customer_id` via `accounts`
3. Once `fact_transaction` is built, add a `balance_agg` CTE to this view sourcing from it

### `customer_age` and `customer_tenure_years`
Intentionally excluded from the view — both are functions of `GETDATE()` and change daily.
Analysts compute them inline:
```sql
FLOOR(DATEDIFF(DAY, birth_date, GETDATE()) / 365.25)               AS customer_age
FLOOR(DATEDIFF(DAY, first_account_opening_date, GETDATE()) / 365.25) AS customer_tenure_years
```

### `managed_accounts`
Excluded from this view. Director / Finance Manager / Authorized Signatory relationships
are available in `bridge_customer_account` for separate analysis if needed.

### Joint account double-counting
Transactions are joined through `bridge_customer_account` which means a joint account
transaction appears for every holder. Financial volume metrics are restricted to
Primary Holder only. Engagement metrics include Joint Holder activity since both
customers genuinely transacted on that account.

---

## Dependencies

---

## Outstanding Items

| Item | Status | Blocked By |
|---|---|---|
| `average_balance` | ⏳ Pending | `fact_transaction` Gold build |
| `avg_balance_quartile` | ⏳ Pending | `fact_transaction` Gold build |
| Business customer 360 | 🔲 Not started | `customers_business` Silver complete |
