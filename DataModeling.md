## Data Warehouse Design (Star Schema)

### Fact Table: `FactBug`
Grain: **one row = one bug (ticket)**

| Measure | Description |
|----------|--------------|
| `IssueCount` | Always 1 (used for counts) |
| `IsOpen` | 1 if bug status = open / new |
| `IsFixed` | 1 if resolution = “corrigé” |
| `IsAssigned` | 1 if developer assigned |
| `AgeDays` | Days between creation and last update |
| `TimeToResolutionDays` | Days between creation and resolution |

### Dimensions

| Dimension | Description | Example values |
|------------|--------------|----------------|
| **DimDate** | Calendar (submission, update, resolution dates) | 2025-10-21 |
| **DimUser** | Reporter and developer identities | ale, jghali |
| **DimProject** | Project name (e.g., Scribus) | Scribus |
| **DimPriority** | Business priority of bug | normale, élevée |
| **DimSeverity** | Technical severity | mineur, majeur, plantage |
| **DimReproducibility** | Reproducibility level | toujours, sans objet |
| **DimCategory** | Module or component | General, UI |
| **DimProductVersion** | Product version | 1.6.4, 1.7.1.svn |
| **DimOS** | Operating system / platform | Windows 11, Ubuntu 24.04 |
| **DimVisibility** | Ticket type | public / privé |
| **DimStatus** | Current workflow state | nouveau, fermé |
| **DimResolution** | Final outcome | corrigé, pas un bug |

---

## Star Schema

<img width="1173" height="1380" alt="Startsc1" src="https://github.com/user-attachments/assets/efff54b1-5a49-4110-a922-a3bb01de304b" />

---