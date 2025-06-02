| Method | Endpoint                                      | Summary                                               | Notes                                                            |
|--------|-----------------------------------------------|-------------------------------------------------------|------------------------------------------------------------------|
| GET    | `/lineage/flat`                               | Get Flat Table Lineage                                | Returns all lineage in a flattened format                       |
| POST   | `/lineage/populate`                           | Populate Lineage Data                                 | Bulk/manual insertion of lineage                                |
| GET    | `/lineage/extract/stage-to-bronze`            | Extract Stage → Bronze Lineage                        | `persist` query param to save to `aud.table_map` and `.source`  |
| GET    | `/lineage/extract/silver-to-gold`             | Extract Silver → Gold Lineage                         | `persist` query param to save to `aud.table_map`                |
| POST   | `/lineage/extract/silver-to-gold`             | Persist Silver → Gold Lineage                         | Accepts mappings in request body                                |
| GET    | `/lineage/extract/silver-to-gold/preview`     | Preview Silver → Gold Procs                           | Dry-run preview of silver→gold lineage                          |
| POST   | `/lineage/load/silver-gold-tables`            | Load Silver-Gold Table Metadata                       | Adds tables to tracking store                                   |
| GET    | `/lineage/view/silver-gold-tables`            | View Tracked Silver-Gold Tables                       | Displays what’s currently in the lineage tracking table         |
| GET    | `/lineage/discover/silver-gold-procs`         | Discover Silver → Gold Stored Procedures              | Lists procs used in gold table creation                         |