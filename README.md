usda-nutrient-db
================

Import USDA Nutrient Database into MongoDB database at:

    mongodb://localhost:27017/usda

Updated to use with Release SR26.

### Usage:

```
mongod --dbpath ./tmp/db  # if needed, start temporary MongoDB server
coffee import.coffee      # Drops existing 'usda' database and imports 
                          # nutrient datafiles from ./data/sr26
```

See LICENSE for details.