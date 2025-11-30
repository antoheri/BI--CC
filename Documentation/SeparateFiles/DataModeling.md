## Data Warehouse Design (Star Schema)

### Fact Table: `FactBug`

Grain: **one row = one bug (ticket)**

| Measure                | Description                           |
| ---------------------- | ------------------------------------- |
| `IssueCount`           | Always 1 (used for counts)            |
| `IsOpen`               | 1 if bug status = open / new          |
| `IsFixed`              | 1 if resolution = “corrigé”           |
| `IsAssigned`           | 1 if developer assigned               |
| `AgeDays`              | Days between creation and last update |
| `TimeToResolutionDays` | Days between creation and resolution  |

### Dimensions

| Dimension              | Description                                     | Example values           |
| ---------------------- | ----------------------------------------------- | ------------------------ |
| **DimCalendar**        | Calendar (submission, update, resolution dates) | 2025-10-21               |
| **DimUser**            | Reporter and developer identities               | ale, jghali              |
| **DimProject**         | Project name (e.g., Scribus)                    | Scribus                  |
| **DimPriority**        | Business priority of bug                        | normale, élevée          |
| **DimSeverity**        | Technical severity                              | mineur, majeur, plantage |
| **DimReproducibility** | Reproducibility level                           | toujours, sans objet     |
| **DimCategory**        | Module or component                             | General, UI              |
| **DimProduct**         | Product version                                 | 1.6.4, 1.7.1.svn         |
| **DimOS**              | Operating system / platform                     | Windows 11, Ubuntu 24.04 |
| **DimVisibility**      | Ticket type                                     | public / privé           |
| **DimStatus**          | Current workflow state                          | nouveau, fermé           |
| **DimResolution**      | Final outcome                                   | corrigé, pas un bug      |

---

## Star Schema

![Star schema diagram would be here](./assets/StarSchema-BI.png)

## SQL DDL Script

The SQL DDL script for creating the above star schema is provided in the `DDLSQL.sql` file.
