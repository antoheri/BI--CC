-- ====================================================================
-- SCHÉMA
-- ====================================================================
CREATE SCHEMA IF NOT EXISTS BI_SCRIBUS COMMENT='Star schema for Scribus bugtracker analytics';
USE SCHEMA BI_SCRIBUS;

-- ====================================================================
-- DIMENSIONS
-- ====================================================================

-- -------------------------
-- DimDate (réutilisée 3x)
-- -------------------------
CREATE OR REPLACE TABLE DimDate (
  DateKey          NUMBER(8,0)    NOT NULL,          -- format YYYYMMDD
  FullDate         DATE           NOT NULL,
  Day              NUMBER(2,0),
  Month            NUMBER(2,0),
  MonthName        VARCHAR(20),
  Quarter          NUMBER(1,0),
  Year             NUMBER(4,0),
  ISOWeek          NUMBER(2,0),
  DayOfWeek        NUMBER(1,0),                      -- 1..7 (ISO: 1=lundi)
  DayName          VARCHAR(20),
  IsWeekend        NUMBER(1,0),                      -- 1/0
  CONSTRAINT PK_DimDate PRIMARY KEY (DateKey)
)
COMMENT='Calendar dimension (YYYYMMDD).';

-- -------------------------
-- DimProject
-- -------------------------
CREATE OR REPLACE TABLE DimProject (
  ProjectKey       NUMBER(38,0)   NOT NULL IDENTITY,
  ProjectName      VARCHAR(200)   NOT NULL,
  ProjectCode      VARCHAR(50),
  CONSTRAINT PK_DimProject PRIMARY KEY (ProjectKey),
  CONSTRAINT UQ_DimProject_ProjectName UNIQUE (ProjectName)
)
COMMENT='Project dimension (mostly Scribus, but extensible).';

-- -------------------------
-- DimUser (utilisée 2x : Reporter / Assignee)
-- -------------------------
CREATE OR REPLACE TABLE DimUser (
  UserKey          NUMBER(38,0)   NOT NULL IDENTITY,
  UserLogin        VARCHAR(120)   NOT NULL,          -- ex: "ale", "jghali"
  UserType         VARCHAR(20),                      -- Reporter / Developer / Both (optionnel)
  DisplayName      VARCHAR(200),                     -- si disponible
  Email            VARCHAR(320),                     -- si disponible
  IsActive         NUMBER(1,0),                      -- 1/0 (optionnel)
  CONSTRAINT PK_DimUser PRIMARY KEY (UserKey),
  CONSTRAINT UQ_DimUser_UserLogin UNIQUE (UserLogin)
)
COMMENT='Person dimension for reporters and developers.';

-- -------------------------
-- DimPriority
-- -------------------------
CREATE OR REPLACE TABLE DimPriority (
  PriorityKey      NUMBER(38,0)   NOT NULL IDENTITY,
  PriorityName     VARCHAR(50)    NOT NULL,          -- basse, normale, élevée, urgente...
  PriorityOrder    NUMBER(3,0)    NOT NULL,          -- permet de trier (1..n)
  CONSTRAINT PK_DimPriority PRIMARY KEY (PriorityKey),
  CONSTRAINT UQ_DimPriority_Name UNIQUE (PriorityName)
)
COMMENT='Priority dimension with explicit ordering.';

-- -------------------------
-- DimSeverity
-- -------------------------
CREATE OR REPLACE TABLE DimSeverity (
  SeverityKey      NUMBER(38,0)   NOT NULL IDENTITY,
  SeverityName     VARCHAR(50)    NOT NULL,          -- mineur, majeur, plantage, fonctionnalité...
  SeverityOrder    NUMBER(3,0)    NOT NULL,
  CONSTRAINT PK_DimSeverity PRIMARY KEY (SeverityKey),
  CONSTRAINT UQ_DimSeverity_Name UNIQUE (SeverityName)
)
COMMENT='Severity dimension with explicit ordering.';

-- -------------------------
-- DimReproducibility
-- -------------------------
CREATE OR REPLACE TABLE DimReproducibility (
  ReproKey         NUMBER(38,0)   NOT NULL IDENTITY,
  ReproName        VARCHAR(80)    NOT NULL,          -- toujours, parfois, aléatoire, sans objet...
  ReproOrder       NUMBER(3,0),
  CONSTRAINT PK_DimReproducibility PRIMARY KEY (ReproKey),
  CONSTRAINT UQ_DimReproducibility_Name UNIQUE (ReproName)
)
COMMENT='Reproducibility dimension.';

-- -------------------------
-- DimCategory
-- -------------------------
CREATE OR REPLACE TABLE DimCategory (
  CategoryKey      NUMBER(38,0)   NOT NULL IDENTITY,
  CategoryName     VARCHAR(200)   NOT NULL,          -- e.g. General, UI, Text Frames...
  CategoryGroup    VARCHAR(200),                     -- regroupement optionnel
  CONSTRAINT PK_DimCategory PRIMARY KEY (CategoryKey),
  CONSTRAINT UQ_DimCategory_Name UNIQUE (CategoryName)
)
COMMENT='Functional area / module dimension.';

-- -------------------------
-- DimProductVersion
-- -------------------------
CREATE OR REPLACE TABLE DimProductVersion (
  ProductVersionKey NUMBER(38,0)  NOT NULL IDENTITY,
  VersionName       VARCHAR(120)  NOT NULL,          -- "1.5.0svn", "1.7.1.svn"
  Major             NUMBER(5,0),                      -- parsé si possible
  Minor             NUMBER(5,0),
  Patch             NUMBER(5,0),
  Channel           VARCHAR(50),                      -- svn, stable, beta...
  ReleaseDate       DATE,                             -- si inférable
  CONSTRAINT PK_DimProductVersion PRIMARY KEY (ProductVersionKey),
  CONSTRAINT UQ_DimProductVersion_Name UNIQUE (VersionName)
)
COMMENT='Product version dimension (source: "Version du produit" / "Résolue dans la version").';

-- -------------------------
-- DimOS
-- -------------------------
CREATE OR REPLACE TABLE DimOS (
  OSKey            NUMBER(38,0)   NOT NULL IDENTITY,
  OSName           VARCHAR(120)   NOT NULL,          -- Windows, macOS, Linux...
  OSVersion        VARCHAR(120),                      -- "10.15.7", "24.10 64-bit"
  Platform         VARCHAR(120),                      -- Desktop, Macintosh, x86_64...
  NormalizedFamily VARCHAR(120),                      -- Windows / macOS / Linux (regroupement)
  CONSTRAINT PK_DimOS PRIMARY KEY (OSKey)
)
COMMENT='Operating system / platform dimension (normalized).';

-- -------------------------
-- DimVisibility
-- -------------------------
CREATE OR REPLACE TABLE DimVisibility (
  VisibilityKey    NUMBER(38,0)   NOT NULL IDENTITY,
  VisibilityName   VARCHAR(50)    NOT NULL,          -- public / privé...
  CONSTRAINT PK_DimVisibility PRIMARY KEY (VisibilityKey),
  CONSTRAINT UQ_DimVisibility_Name UNIQUE (VisibilityName)
)
COMMENT='Ticket visibility dimension.';

-- -------------------------
-- DimStatus
-- -------------------------
CREATE OR REPLACE TABLE DimStatus (
  StatusKey        NUMBER(38,0)   NOT NULL IDENTITY,
  StatusName       VARCHAR(80)    NOT NULL,          -- nouveau, affecté, traité, fermé...
  IsTerminal       NUMBER(1,0)    NOT NULL DEFAULT 0,
  StatusOrder      NUMBER(3,0),
  CONSTRAINT PK_DimStatus PRIMARY KEY (StatusKey),
  CONSTRAINT UQ_DimStatus_Name UNIQUE (StatusName)
)
COMMENT='Ticket status dimension (terminal vs non-terminal).';

-- -------------------------
-- DimResolution
-- -------------------------
CREATE OR REPLACE TABLE DimResolution (
  ResolutionKey    NUMBER(38,0)   NOT NULL IDENTITY,
  ResolutionName   VARCHAR(80)    NOT NULL,          -- corrigé, pas un bug, ouvert...
  IsSuccessfulFix  NUMBER(1,0)    NOT NULL DEFAULT 0, -- 1 si "corrigé"
  ResolutionOrder  NUMBER(3,0),
  CONSTRAINT PK_DimResolution PRIMARY KEY (ResolutionKey),
  CONSTRAINT UQ_DimResolution_Name UNIQUE (ResolutionName)
)
COMMENT='Ticket resolution outcome dimension.';

-- ====================================================================
-- TABLE DE FAITS
-- ====================================================================

CREATE OR REPLACE TABLE FactBug (
  FactBugId              NUMBER(38,0)   NOT NULL IDENTITY,  -- surrogate PK
  BugId                  NUMBER(38,0)   NOT NULL,           -- identifiant Mantis (dégénéré)
  -- Dates (FK vers DimDate)
  DateSubmissionKey      NUMBER(8,0)    NOT NULL,
  DateLastUpdateKey      NUMBER(8,0)    NOT NULL,
  DateResolvedKey        NUMBER(8,0),                        -- nullable
  -- Dimensions (FK)
  ProjectKey             NUMBER(38,0)   NOT NULL,
  ReporterKey            NUMBER(38,0)   NOT NULL,            -- DimUser
  AssigneeKey            NUMBER(38,0),                        -- DimUser (nullable)
  PriorityKey            NUMBER(38,0)   NOT NULL,
  SeverityKey            NUMBER(38,0)   NOT NULL,
  ReproKey               NUMBER(38,0)   NOT NULL,
  CategoryKey            NUMBER(38,0)   NOT NULL,
  ProductVersionKey      NUMBER(38,0),                        -- "Version du produit"
  ResolvedInVersionKey   NUMBER(38,0),                        -- "Résolue dans la version"
  OSKey                  NUMBER(38,0),                        -- nullable (souvent manquant)
  VisibilityKey          NUMBER(38,0)   NOT NULL,
  StatusKey              NUMBER(38,0)   NOT NULL,
  ResolutionKey          NUMBER(38,0)   NOT NULL,

  -- Attributs DD utiles (optionnels pour facil. de recherche)
  Summary                VARCHAR(2000),                       -- "Résumé"
  -- Mesures stockées (brutes)
  IssueCount             NUMBER(1,0)    NOT NULL DEFAULT 1,
  IsAssigned             NUMBER(1,0)    NOT NULL DEFAULT 0,   -- 1 si AssigneeKey non NULL (renseigné à l’ETL)
  IsOpen                 NUMBER(1,0)    NOT NULL DEFAULT 0,   -- 1 si statut "ouvert/nouveau/affecté..." (règle ETL)
  IsFixed                NUMBER(1,0)    NOT NULL DEFAULT 0,   -- 1 si Résolution = "corrigé" (règle ETL)
  AgeDays                NUMBER(10,0),                         -- Mis à jour - Soumission (ou Now - Soumission)
  TimeToResolutionDays   NUMBER(10,0),                         -- Résolution - Soumission (nullable)

  CONSTRAINT PK_FactBug PRIMARY KEY (FactBugId),

  -- Unicité sur l’identifiant métier (si 1:1 CSV -> fait)
  CONSTRAINT UQ_FactBug_BugId UNIQUE (BugId),

  -- Références (déclaratives; non-enforced en Snowflake)
  CONSTRAINT FK_FactBug_DateSubmission   FOREIGN KEY (DateSubmissionKey)    REFERENCES DimDate(DateKey),
  CONSTRAINT FK_FactBug_DateLastUpdate   FOREIGN KEY (DateLastUpdateKey)    REFERENCES DimDate(DateKey),
  CONSTRAINT FK_FactBug_DateResolved     FOREIGN KEY (DateResolvedKey)      REFERENCES DimDate(DateKey),

  CONSTRAINT FK_FactBug_Project          FOREIGN KEY (ProjectKey)           REFERENCES DimProject(ProjectKey),
  CONSTRAINT FK_FactBug_Reporter         FOREIGN KEY (ReporterKey)          REFERENCES DimUser(UserKey),
  CONSTRAINT FK_FactBug_Assignee         FOREIGN KEY (AssigneeKey)          REFERENCES DimUser(UserKey),
  CONSTRAINT FK_FactBug_Priority         FOREIGN KEY (PriorityKey)          REFERENCES DimPriority(PriorityKey),
  CONSTRAINT FK_FactBug_Severity         FOREIGN KEY (SeverityKey)          REFERENCES DimSeverity(SeverityKey),
  CONSTRAINT FK_FactBug_Repro            FOREIGN KEY (ReproKey)             REFERENCES DimReproducibility(ReproKey),
  CONSTRAINT FK_FactBug_Category         FOREIGN KEY (CategoryKey)          REFERENCES DimCategory(CategoryKey),
  CONSTRAINT FK_FactBug_ProductVersion   FOREIGN KEY (ProductVersionKey)    REFERENCES DimProductVersion(ProductVersionKey),
  CONSTRAINT FK_FactBug_ResolvedVersion  FOREIGN KEY (ResolvedInVersionKey) REFERENCES DimProductVersion(ProductVersionKey),
  CONSTRAINT FK_FactBug_OS               FOREIGN KEY (OSKey)                REFERENCES DimOS(OSKey),
  CONSTRAINT FK_FactBug_Visibility       FOREIGN KEY (VisibilityKey)        REFERENCES DimVisibility(VisibilityKey),
  CONSTRAINT FK_FactBug_Status           FOREIGN KEY (StatusKey)            REFERENCES DimStatus(StatusKey),
  CONSTRAINT FK_FactBug_Resolution       FOREIGN KEY (ResolutionKey)        REFERENCES DimResolution(ResolutionKey)
)
COMMENT='Grain = 1 row per bug (current snapshot from CSV). Measures: counts & basic durations.';

-- ====================================================================
-- VUES D’AIDE (KPI fréquents)
-- ====================================================================
-- Exemple : vue compacte des mesures et axes principaux
CREATE OR REPLACE VIEW V_FactBug_Core AS
SELECT
  f.BugId,
  f.DateSubmissionKey,
  f.DateLastUpdateKey,
  f.DateResolvedKey,
  f.ProjectKey,
  f.ReporterKey,
  f.AssigneeKey,
  f.PriorityKey,
  f.SeverityKey,
  f.ReproKey,
  f.CategoryKey,
  f.ProductVersionKey,
  f.ResolvedInVersionKey,
  f.OSKey,
  f.VisibilityKey,
  f.StatusKey,
  f.ResolutionKey,
  f.IssueCount,
  f.IsAssigned,
  f.IsOpen,
  f.IsFixed,
  f.AgeDays,
  f.TimeToResolutionDays
FROM FactBug f;
