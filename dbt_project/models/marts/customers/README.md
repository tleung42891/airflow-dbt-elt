# Customer Metrics dbt Project

## Overview

This dbt project provides monthly metrics for **Active Customers**, **MRR (Monthly Recurring Revenue)**, and **Churn**. The project follows a layered architecture with staging, intermediate, and mart models to ensure data quality and maintainability.

The project integrates three data sources:
- **Customers**: Customer signup dates, status, and region
- **Plans**: Subscription plan definitions and pricing
- **Subscriptions**: Customer subscriptions linking customers to plans with start/end dates

## Project Structure

```
models/
├── staging/
│   └── customers/
│       ├── stg_customers.sql           # Standardizes raw customer data
│       ├── stg_plans.sql               # Standardizes plan pricing data
│       ├── stg_subscriptions.sql       # Standardizes subscription data
│       └── _schema.yml                 # Tests and documentation
├── intermediate/
│   └── customers/
│       ├── int_subscription_snapshots.sql      # Monthly subscription snapshots with pricing
│       ├── int_customer_status_changes.sql     # Monthly customer snapshots
│       └── _schema.yml                         # Tests and documentation
└── marts/
    └── customers/
        ├── mart_active_customers_monthly.sql  # Active customer metrics
        ├── mart_mrr_monthly.sql               # MRR metrics
        ├── mart_logo_churn_monthly.sql        # Logo churn metrics
        └── _schema.yml                        # Tests and documentation
```

## Model Grain and Design Choices

### Grain

- **Staging**:
  - `stg_customers`: One row per customer (deduplicated)
  - `stg_plans`: One row per plan (deduplicated)
  - `stg_subscriptions`: One row per subscription (deduplicated)
- **Intermediate**:
  - `int_subscription_snapshots`: One row per subscription per month (from start to end date)
  - `int_customer_status_changes`: One row per customer per month (from signup to current month)
- **Marts**: One row per month per region

### Design Rationale

#### 1. Staging Layer

##### `stg_customers`
- **Purpose**: Clean and standardize raw customer data
- **Key transformations**:
  - Deduplicates customers using `ROW_NUMBER()`
  - Ensures data types are consistent (dates, strings)
- **Why**: Provides a clean foundation for downstream models

##### `stg_plans`
- **Purpose**: Clean and standardize plan pricing data
- **Key transformations**:
  - Ensures price is numeric
  - Validates required fields
- **Why**: Provides accurate pricing for MRR calculations

##### `stg_subscriptions`
- **Purpose**: Clean and standardize subscription data
- **Key transformations**:
  - Deduplicates subscriptions using `ROW_NUMBER()`
  - Ensures date formats are consistent
  - Validates foreign key relationships (customer_id, plan_id)
- **Why**: Links customers to plans with accurate start/end dates for MRR and churn tracking

#### 2. Intermediate Layer

##### `int_subscription_snapshots`
- **Purpose**: Create monthly snapshots of active subscriptions with pricing
- **Key transformations**:
  - Joins subscriptions with plans to get pricing
  - Joins with customers to get region
  - Generates all months from subscription start to end (or current month if active)
  - Calculates MRR per subscription per month
  - Flags active status and churn per month
- **Why**: This is the **primary source** for accurate MRR calculations and subscription-based metrics. Provides actual pricing from plans rather than assumptions.

##### `int_customer_status_changes`
- **Purpose**: Create monthly snapshots of customer status
- **Key transformations**:
  - Uses subscription data when available
  - Falls back to customer status for customers without subscriptions
  - Generates all months from signup to current month
- **Why**: Provides customer-level metrics when subscription data is incomplete

#### 3. Mart Layer

##### `mart_active_customers_monthly`
- **Grain**: Month × Region
- **Metrics**:
  - `active_customers`: Count of distinct active customers from subscriptions
  - `new_customers`: Customers who signed up in the month
  - `churned_customers`: Customers who churned in the month
- **Data Source**: Uses subscription data when available
- **Trade offs**: Doesn't falls back to customer status (should be safe here since it's downstream models, but pipeline or system data COULD be incomplete)
- **Why**: Provides a clear view of customer growth and retention by region

##### `mart_mrr_monthly`
- **Grain**: Month × Region
- **Metrics**:
  - `active_customers`: Count of active customers
  - `active_subscriptions`: Count of active subscriptions
  - `total_mrr`: Total monthly recurring revenue (calculated from actual plan pricing)
  - `new_mrr`: MRR from new subscriptions that started in the month
  - `churned_mrr`: MRR lost from subscriptions that churned in the month
  - `avg_mrr_per_customer`: Average MRR per customer
- **Data Source**: Uses actual subscription and plan pricing data (no assumptions)
- **Trade offs**: Doesn't falls back to potential dynamic changes over time. Static based on subscriptions realtime information. Could be off if prices of each plan fluctuates over time
- **Why**: Provides revenue metrics based on real subscription data.

##### `mart_churn_monthly`
- **Grain**: Month × Region
- **Metrics**:
  - `churned_customers`: Count of customers who canceled
  - `churn_rate_percent`: Churn rate as % of previous month's active customers
- **Why**: Churn is a key metric for understanding customer retention

## Tradeoffs and Assumptions

### 1. Subscription vs Customer Status
**Approach**: Optimized for customer data when it's customer based, falls back to subscription data when it's revenue/over time analysis

**Rationale**: 
- Subscription data provides accurate start/end dates and pricing
- Customer status could be a fallback for customers without subscription records
- This hybrid approach ensures we capture all customers while maximizing accuracy (if there is a problem in the data)

**Impact**:
-  Accurate MRR from actual subscription pricing
-  Accurate churn dates from subscription end dates
-  Less accurate Customers without subscriptions rely on status field

### 2. MRR Calculation
**Approach**: Uses actual subscription and plan pricing data.

**Rationale**: 
- Joins subscriptions with plans to get real pricing (assuming this is operating in real time and never changes)
- Calculates MRR per subscription per month

**Impact**:
-  Accurate MRR based on real data
-  Accounts for subscription start/end dates
-  Less accurate if prices are flexible with more complex calculations via potential dynamic/promo pricing

### 3. Churn Rate Calculation
**Approach**: Uses only `int_customer_status_changes` (customer status-based tracking)

**Rationale**: 
- Relies on customer status field which only contains current state (active/canceled)
- Uses previous month's active customer count as denominator for churn rate

**Drawbacks for this model**: (I didn't want to over-assume and potentially model out additional hundreds of lines and go down the rabbit hole in potenitla edge cases):
- **No historical churn dates**: Since customer status only shows current state, we are not accurately determine when a customer actually churned in the past. The model assumes canceled customers churned in the current month only.
- **Limited churn timing accuracy**: Without subscription end dates, we cannot track the exact month a customer canceled if they canceled in a previous period.
- **Potential double-counting risk**: If a customer's status changes multiple times, the model may not accurately reflect the true churn timeline.
- **Churn rate denominator limitations**: Uses previous month's active count, which may not account for mid-month churns or reactivations accurately.

**Recommendation**: 
- Consider using `int_subscription_snapshots` for more accurate churn tracking based on subscription end dates
- Implement cohort-based analysis for more sophisticated churn rate calculations
- Add a `cancel_date` field to the customer table for accurate historical churn tracking

### 4. Data Quality & Testing Strategy
**Assumptions**:
- Customer IDs, Plan IDs, and Subscription IDs are unique
- Subscription start dates are valid and not null
- Subscription end dates may be null (for active subscriptions)
- Status values follow expected patterns (active, canceled, expired)

### Source Tests
- `ids`: unique, not_null
- `date`: not_null

 Staging Tests
- All source tests plus data type validations
- Deduplication verification

### Intermediate Tests
- Monthly snapshot completeness
- Status flag logic validation

### Mart Tests
- Not null checks on key metrics
- Data type validations
- Business logic validation (e.g., MRR = customers × price)

## Potential Enhancements
1. **Add cohort analysis models** for more sophisticated retention metrics
2. **Implement incremental models** for better performance on large datasets
3. **Add data quality monitoring** with dbt-expectations or custom tests
4. **Create unified customer metrics mart** combining all three metrics for easier analysis (reduce to two vs three marts)
5. **Add subscription upgrade/downgrade tracking** to capture MRR changes from plan changes
6. **Add plan change history** to track when customers switch between plans
