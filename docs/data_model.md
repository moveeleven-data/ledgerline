## Grains & Keys (Facts)

### Usage feed
- **Entity Grain:** customer x product x plan (subscription)
- **Row Grain:** customer × product × plan × date 
- **Key:** usage_hkey = subscription + date  
- **Diff:** adds units + included_units to capture metric changes

### Daily price book
- **Entity Grain:** product x plan
- **Row Grain:** product × plan × price_date  
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