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

The bus matrix shows how our single fact table (`fact_usage`) connects to conformed dimensions.  

Rows represent business processes (facts), columns are the shared dimensions that provide consistent slicing.  

| business process | date | customer | product | plan | currency | country |
|------------------|------|----------|---------|------|----------|---------|
| fact_usage       |  X   |    X     |    X    |  X   |    X     |    X    |

**Notes**  
- Ledgerline is scoped around a single daily usage fact for clarity.  
- All dimensions are conformed and consistently joinable.  
- The design leaves open space for future facts (e.g., `fact_billing`, `fact_margin`) to plug into the same shared dimensions.  
- Pricing is applied during fact construction from the daily price book, but because analysts do not slice by price directly, it is not modeled as a dimension here.