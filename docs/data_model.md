## Grains & Keys (Facts)

### Usage feed
- **Logical Grain:** customer x product x plan (subscription)
- **Physical Grain:** customer × product × plan × date 
- **Key:** usage_hkey = subscription + date  
- **Diff:** adds units + included_units to capture metric changes

### Daily price book
- **Logical Grain:** product x plan
- **Physical Grain:** product × plan × price_date  
- **Key:** price_book_hkey = product + plan + date  
- **Diff:** adds unit_price to capture changes

---

## History Grains

### Usage history
- **Grain:** one row per subscription × date (same as usage_hkey)  
- **Row type:**  
  - `OPEN` = row exists in feed  
  - `CLOSE_SYNTHETIC` = key was open yesterday but missing today  
- **Diff behavior:** CLOSE rows force metrics to zero to guarantee a new version  
- **Rule:** never both OPEN and CLOSE for the same key/date  

### Dimension histories
- **Grain:** one row per dimension key per version  
- **Versioning:** new row whenever the diff hash changes  
- **Latest version:** the row with the most recent load timestamp  

---

## Enterprise Data Warehouse Bus Matrix

The bus matrix shows how our single fact table (`fact_usage`) connects to shared dimensions.  

- **Rows** represent business processes (facts).  
- **Columns** represent conformed dimensions that provide consistent slicing.  

| business process | date | customer | product | plan | currency | country |
|------------------|------|----------|---------|------|----------|---------|
| fact_usage       |  X   |    X     |    X    |  X   |    X     |    X    |

### Notes
- Ledgerline currently focuses on a single daily usage fact for simplicity.  
- All listed dimensions are conformed and consistently joinable.  
- The design allows for future facts (e.g., `fact_billing`, `fact_margin`) to plug into the same shared dimensions.  
- Pricing comes from the daily price book during fact construction. Because analysts wouldn't typically slice data by price directly, it is not modeled as a separate dimension.  
