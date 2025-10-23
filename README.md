# Scribus Bugtracker BI Project

## Overview

This project is part of the **Data Exploitation (BI)** course (Module 62-62, HES-SO).  
It aims to **analyze the Scribus bug tracking system** using a **data warehouse model** and **OLAP cube operations**.

The data comes from the **Mantis Bugtracker** used by the open-source project [Scribus](https://www.scribus.net).  
Each record represents a reported bug with information about its reporter, assignee, category, priority, severity, reproducibility, version, operating system, and resolution status.

---

## Data Source

- **File name:** `Scribus.csv`  
- **Records:** 3,254  
- **Columns:** 19  
- **Source:** Extracted weekly from [http://teachingse.hevs.ch/csvFiles/](http://teachingse.hevs.ch/csvFiles/)  
- **System:** [https://bugs.scribus.net](https://bugs.scribus.net)

### Key Columns

| Column | Description |
|---------|--------------|
| `Identifiant` | Unique bug ID (from Mantis) |
| `Rapporteur` | User who reported the bug |
| `Affecté à` | Developer assigned to fix it |
| `Priorité` | Importance level (basse, normale, élevée…) |
| `Sévérité` | Severity (mineur, majeur, plantage, fonctionnalité…) |
| `Reproductibilité` | Reproducibility (toujours, parfois, sans objet…) |
| `Version du produit` | Product version where the bug appeared |
| `Catégorie` | Functional module (UI, Text Frames, General, etc.) |
| `Date de soumission` | Date when the bug was reported |
| `Mis à jour` | Last modification date |
| `Système d’exploitation` | OS used when reporting |
| `Plateforme` | Machine type (Linux, Desktop, Macintosh…) |
| `État` | Current state (nouveau, traité, fermé, affecté…) |
| `Résolution` | Final outcome (corrigé, pas un bug, ouvert…) |
| `Résumée` | Short text summary of the issue |

---

## Data Analysis Summary

- **Total records:** 3,254 bugs  
- **Distinct reporters:** 864 users  
- **Assigned bugs:** 444 (~13%) have a developer assigned  
- **Most frequent priority:** `normale` (92%)  
- **Most frequent severity:** `mineur` (47%)  
- **Time range:** 2003 → 2025  
- **Public tickets:** 100% (Visibilité = public)  
- **Operating systems:** Ubuntu, Windows, macOS, Fedora…  
- **Most common categories:** `General`, `User Interface`, `Build System`

### Observations

- A large portion of tickets are still open (`nouveau`, `ouvert`).
- Resolution `corrigé` appears in only 2% of records.
- Many fields are optional or incomplete (`Affecté à`, `Résolue dans la version`, OS details).
- The dataset represents a **snapshot** (current state), not a daily history.

---

## Business Intelligence Objectives

The goal is to transform the raw bug data into a **data warehouse** with a **Star Schema**, enabling multidimensional analysis in **Snowflake** or **Power BI**.

---

## Analytical Questions

The following questions guide the design of the cube and the measures:

### 1. General Activity
- How many bugs have been created over time?
- What is the trend of new vs. resolved bugs by month or year?
- How many bugs are still open today?

### 2. Product Quality
- Which versions of Scribus generate the most bugs?
- Which versions contain the most corrected bugs?
- What is the bug correction rate per version?

### 3. Developer Performance
- How many bugs are assigned to each developer?
- Which developers resolve the most bugs?
- What is the average resolution time per developer?

### 4. Functional Analysis
- Which categories (modules) generate the most bugs?
- Which categories have the highest ratio of open vs. fixed bugs?
- What are the most common severity levels per category?

### 5. Environment & Reproducibility
- Which operating systems generate the most bug reports?
- Are some OS families (Windows, macOS, Linux) more error-prone?
- What percentage of bugs are always reproducible?

---

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

## Example KPIs

| KPI | Definition | Formula |
|-----|-------------|----------|
| **Total Bugs** | Number of bug records | `COUNT(*)` |
| **Fixed Bugs** | Bugs with Resolution = “corrigé” | `SUM(IsFixed)` |
| **Open Bugs** | Bugs still open or new | `SUM(IsOpen)` |
| **Fix Rate** | % of bugs fixed | `(Fixed / Total) * 100` |
| **Avg Resolution Time** | Mean days between submission and fix | `AVG(TimeToResolutionDays)` |
| **Assigned Rate** | % of bugs with assignee | `(Assigned / Total) * 100` |

---

## Tools & Workflow

- **Data source:** CSV from Mantis/Scribus  
- **Storage & Queries:** Snowflake (Enterprise Edition, EU-Frankfurt)  
- **ETL Steps:**  
  1. Load CSV → staging table  
  2. Clean & normalize (priorities, OS names, versions)  
  3. Populate dimension tables  
  4. Load `FactBug` with surrogate keys  
- **Visualization:** Power BI, Cube viewers, MDX queries  
- **Version control:** dbdiagram.io for schema design  

---

## Deliverables

1. **Star Schema** implemented in Snowflake  
2. **ETL scripts** to load dimensions and facts  
3. **Power BI / OLAP reports** answering analytical questions  
4. **Documentation & README** (this file)

---

## Star Schema

<img width="1173" height="1380" alt="Startsc1" src="https://github.com/user-attachments/assets/efff54b1-5a49-4110-a922-a3bb01de304b" />

---

## References

- Scribus Bugtracker: [https://bugs.scribus.net](https://bugs.scribus.net)
- MantisBT project: [https://www.mantisbt.org](https://www.mantisbt.org)
- HES-SO Course Material (Prof. René Schumann)
- Snowflake Workshop (Gaël Charrière)

---

*Author: [Your Name]*  
*Data Exploitation — HES-SO Valais Wallis, 2025*
