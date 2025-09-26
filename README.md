<h1 align="center">Ledgerline: Margin & Growth Analytics</h1>

<p align="center">
  Transform raw subscription usage into revenue, margin, and growth insights with dbt and Snowflake.
  <br/><br/>
</p>

## Business Story

Ledgerline simulates the financial heartbeat of a B2B SaaS platform called **Atlas**. 

Customers subscribe to products, get a bundle of included usage, and pay overage once they cross that threshold.  

Atlas offers three core services: 
- **PROD-API** - Lets customers call the Atlas API  
- **PROD-ETL** - Lets customers process data rows  
- **PROD-ALRT** - Lets customers send alerts and notifications  

Pricing is not static. Each day, a price book sets the unit rate for each product and plan.  

And, every night, the metering system emits a usage log, a running tally of what customers actually did.

> *Customer X made 12,000 API calls on 2025-09-16.*  

> *Customer Y processed 250,000 ETL rows on the same day.*  

Ledgerline transforms that feed into a **star schema**, a daily usage fact table connected to five dimensions.  

The star schema makes the business questions easy to answer:  
- **Finance** can see recurring revenue and how much comes from overages.  
- **Product** can see which services are growing and how pricing changes affect usage.  
- **Customer Success** can see which accounts are hitting limits, showing upsell chances or churn risk.  
- **Executives** can see growth by region and the impact of new products like Alerts.  

---

## Atlas Data Model

Atlas runs on a **star schema**.

At the core, one fact table logs daily subscription usage and billing. It captures units consumed, plan coverage, and overages.

It is the system of record for turning activity into dollars.  

Five conformed dimensions provide context:  
- **Customer** - identity and geography  
- **Product & Plan** - the commercial catalog  
- **Currency** - consistent amounts  
- **Country** - rollups by market

The diagram below shows how the fact and dimensions connect into a simple, reliable model.

![Ledgerline Architecture](docs/assets/erd_physical_model_2.png)

Atlas reduces everything to one clear structure: daily activity measured and explained.

Every team sees the **same story** in the **same numbers**.

From raw usage to revenue, nothing gets lost in translation.

---

## Project Layout

  **[models/](models/)** - core transformation layers.  
  - **sources/** - Declares runtime sources (Atlas metering feed).  
  - **staging/** - Normalizes seeds/sources, deduplicates, adds surrogate keys.  
  - **history/** - Persists full change logs (SCD2 reference history).  
  - **refined/** - Collapses history into current views.  
  - **marts/usage/** - Publishes the Usage Mart.

  **[macros/](macros/)** - reusable utilities.  
  - **core/** - Pure helpers (date normalization, string cleanup).  
  - **delivery/** - Presentation utilities (self-completing dimensions).  
  - **dev_utils/** - Local iteration helpers (insert/delete test rows).  
  - **history/** - Toolkit for state persistence.  
  - **migrations/** - Versioned DDL.  
  - **tests/** - Generic test definitions.

  **[seeds/](seeds/)** - versioned reference CSVs.

  **[analyses/](analyses/)** - ad-hoc SQL.  
  - **qa/** - Audit probes and diagnostics.  
  - **dev/** - Scratch queries for local iteration and macro testing.

  **[tests/](tests/)** - singular tests.

  **[docs/](docs/)** - ERDs, diagrams, and BI references.

---

<p align="center">Designed and maintained by <a href="https://github.com/moveeleven-data">Matthew Tripodi</a></p>
