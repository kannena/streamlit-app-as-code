# I Stopped Building Dashboards. I Built a Dashboard Engine Instead.

**After years of building reporting solutions across different tools and platforms, one pattern became impossible to ignore: we were not building new reporting apps — we were rebuilding the same app again and again.**

Different report names. Different tables. Different filters. But the same core flow every time:

**Filter the data. Build the query. Apply security. Show results. Export. Log activity.**

At some point, the real problem was no longer dashboard performance. It was **dashboard repetition**.

So we stopped building dashboards one by one — and built a **dashboard engine** instead.

## The Wall We Hit with Traditional BI

Our team needed **heavy operational reporting**.

Not executive dashboards with a few slicers. Not summary KPIs. These were dense, multi-year transactional reports that operations teams use every day to search, validate, export, and act on data. Millions of rows. Complex filters. High user expectations.

We started with Power BI because it already existed in the enterprise, and for many visualization use cases, it is the right tool.

But for this workload, we kept hitting limits.

Import-based models were not a good fit for large operational detail. DirectQuery removed some of that pressure, but every interaction still had a cost. Users felt the delay. And once users feel delay in an operational workflow, adoption starts dropping very quickly.

The bigger gap was not just scale. It was interaction.

Our users did not only need to view data. They needed to **work with it**.

They needed to update control tables, adjust service charge settings, flag records for review, and add notes from the same screen where they were analyzing the data.

That is where the mismatch became clear.

Power BI is excellent for visualization. But we were trying to use a visualization tool as an **operational application**.

## The Shift to Streamlit in Snowflake

That is when **Streamlit in Snowflake** became the right answer for us.

Instead of moving data out to another tool, the application runs where the data already lives. The compute stays close to the data. Security stays within Snowflake. Governance stays within the same platform.

Three things stood out immediately.

**Compute close to data.** There was no separate data movement problem to solve. The app queried Snowflake directly.

**Native write-back.** Since the app was built in Python, actions like `INSERT`, `UPDATE`, and `MERGE` were no longer special features to negotiate around. They were just part of the application logic.

**Unified identity and access.** Users came through the same identity and access model instead of managing a separate application login flow.

The stack also felt practical. Python and SQL were already familiar. There was no need to build and maintain a separate frontend framework just to support operational use cases.

For our needs, it was the right platform.

## Then We Created a New Problem

Streamlit solved the platform problem.

But then we ran into an engineering problem.

Every new report started from the previous app. Copy the code. Rename variables. Change filters. Adjust queries. Deploy. Repeat.

At first, this felt fast.

Then the cost showed up.

One app was for billing adjustments. Another was for transaction history. Another was for customer account lookups. Different business use cases, same underlying pattern.

After enough apps, we had multiple Python codebases that looked almost the same but behaved slightly differently. Filters were not fully consistent. Export options varied. Pagination existed in one app but not another. Security logic was duplicated. Audit behavior was uneven.

We had replaced one bottleneck with another.

That was the real turning point.

The question stopped being, **“Can we build another app?”**

The better question became, **“Why are we still building each of these manually?”**

## The App-as-Code Idea

Once I stepped back, the pattern became obvious.

Across all these apps, the core logic was the same — filter, query, secure, display, export, audit. Every time. The only things that changed were which filters, which tables, and which security rules.

So I separated the two.

The **what** became configuration.

The **how** became one reusable engine.

That is the model I now call **App-as-Code**.

A new app no longer starts with writing UI code. It starts with defining the app in YAML and SQL. The Python engine handles the rest.

**Write the config. Write the query template. Push to Git. Deploy the app.**

That one change completely reshaped the delivery model.

## How the Engine Works

The framework is built around four simple pieces.

### 1. YAML Configuration

Each app is defined in a YAML file.

That config describes the filters, their order, their type, dependencies, export settings, pagination, audit behavior, and other app options.

Instead of writing Python to build the interface, the developer describes it declaratively. The UI is generated automatically.

Here is a simplified example:

```yaml
filters:
  territory:
    label: "Territory"
    input_type: "checkbox"
    sql: "SELECT DISTINCT territory FROM {DB}.corp.dim_org_hierarchy"
    mandatory: true
    order: 1

  department:
    label: "Department"
    depends_on: ["territory"]
    input_type: "checkbox"
    sql: "SELECT DISTINCT dept WHERE territory IN ({territory})"
    order: 2

  date_range:
    label: "Date Range"
    input_type: "date"
    date_pattern: "range"
    date_column: "transaction_date"
    order: 3
```

### 2. SQL Templates

Each app has a SQL template. At runtime, the framework injects filter selections, security restrictions, and pagination automatically. Environment placeholders (`{DB}`) resolve based on deployment target, so the same definition works across DEV, QA, and production without manual rewiring:

```sql
WITH security_filters AS (
    SELECT DISTINCT region_code
    FROM {DB}.security.user_division_access
    WHERE user_login_id = '{current_user}'
),
base AS (
    SELECT customer_name, order_date, amount, status
    FROM {DB}.analytics.orders
    -- WHERE_PLACEHOLDER
)
SELECT * FROM base
WHERE region_code IN (SELECT region_code FROM security_filters)
```

### 3. One Python Engine

A single Python engine reads the YAML, resolves filter dependencies (including AND/OR cascading logic), builds the query, applies Row-Level Security, executes against Snowflake, renders results, supports exports, and logs user activity. App developers focus on business logic, not plumbing.

### 4. GitHub Actions for CI/CD

The deployment model became just as important as the app model.

A GitHub Actions pipeline detects what changed and deploys only what is needed. Framework changes can redeploy all apps. App-level changes can deploy only one app. Promotion across environments is tied to branch flow, not manual handling.

The result is simple:

**Git push to live app.**

No manual uploads. No repeated deployment steps. Full traceability in source control.

## What Made It Enterprise-Ready

The framework only became truly useful once it handled the real-world issues that usually break shared platforms.

**Caching** was one of them.

In interactive apps, the same metadata and security lookups can run again and again during a single session. We added a session cache layer so user identity, environment detection, security divisions, and similar lookups are reused instead of repeatedly queried. That removed a large amount of unnecessary database chatter.

**Security** was another.

Instead of trusting every app developer to implement filtering correctly, **Row-Level Security** moved into the framework layer. The engine reads the current user context and injects the required restrictions automatically. That made security a built-in behavior, not a copy-paste responsibility.

**Audit logging** also moved into the framework. Every query execution, export, and key user action is logged centrally using async batch processing — so audit writes never block the UI. Compliance gets complete trails; users notice nothing.

**Saved filter presets** let users save and share their filter combinations across apps through a role-based folder hierarchy. A team lead configures a complex filter once; every team member loads it in one click.

We also added persistent disclaimer acceptance, subscription scheduling, and cross-app filter handoff. The important part was not each feature by itself — it was that once a capability existed in the framework, every app got it for free.

That is the real leverage of platform thinking.

## The Impact

This shift changed more than development speed.

Before the framework, each new report felt like a mini project. After the framework, a new report became mostly configuration.

▸ Time to build a new report: Weeks → Under 1 hour
▸ Deployment: Manual upload → Git push → CI/CD
▸ UI consistency: Mostly similar → Consistent by design
▸ Security: Copy-paste per app → Framework-enforced
▸ Audit trail: Optional → Automatic on everything
▸ Version control: Occasional → Every change, every PR
▸ Export formats: Varies by app → CSV, Excel, PDF on all

The biggest win was not only speed. It was removing repeated engineering decisions from every new app.

And maybe the most important change was this:

The conversation changed.

Teams stopped asking, **“Can engineering build this report?”**

They started asking, **“What should the config look like?”**

That is a very different place to operate from.

## This Is Not a Power BI vs Streamlit Argument

To be clear, this is not about saying Power BI is bad.

Power BI is excellent for the use cases it is meant to serve. It is strong for self-service reporting, visualization, Microsoft ecosystem integration, and broad business consumption.

This framework solves a different problem.

It is for cases where users need a **data application**, not just a dashboard. Where **read and write** happen in the same interface. Where Python logic matters. Where Git-based change control matters. Where CI/CD matters. Where repeatability matters.

These tools are not enemies. They solve different classes of problems.

## What Comes Next

The next step for this framework is making it even more declarative.

I am exploring **AI-assisted filter suggestions**, config-driven visualization support, and broader packaging patterns so teams can adopt the model faster.

But the main lesson is already clear.

The biggest improvement did not come from writing smarter dashboard code.

It came from stopping the cycle of rebuilding the same patterns over and over.

After enough years in engineering, that is the lesson that keeps repeating:

**the best solution is not the one that solves today’s app. It is the one that makes the next 20 apps easier to build.**

If your team is dealing with the same gap between analytics and operational applications, I would genuinely like to hear how you are approaching it.

---

## Try It Yourself

I have open-sourced a sanitized version of the core engine with sample apps, mock Snowflake data, and a working CI/CD pipeline:

**🔗 [GitHub Repository Link]**

The repo includes:
- The complete framework engine (Python)
- Two sample apps with YAML configs and SQL templates (simple + complex with AND/OR filter dependencies)
- Snowflake DDL scripts and seed data to create a working mock environment
- GitHub Actions workflow for CI/CD with smart change detection

---

**How is your team handling the space between dashboards and full custom apps?**

#DataEngineering #Snowflake #Streamlit #Python #AppAsCode #OpenSource #Analytics #CI_CD #GitOps #DataArchitecture