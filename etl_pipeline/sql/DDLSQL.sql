/*
================================================================
 Script de création de la base de données (DWBugs)
 Version 3 - Corrigée
================================================================
*/

-- Vérifie si la DB existe avant de la créer
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DWBugs')
BEGIN
    CREATE DATABASE DWBugs;
END;
GO

-- Change le contexte pour la base de données DWBugs
USE DWBugs;
GO

-- =============================================
-- ===         TABLES DE DIMENSIONS          ===
-- =============================================

-- Dimension Calendrier
-- CORRIGÉ : PAS d'IDENTITY, l'ID est généré par le script (ex: 20251024)
CREATE TABLE dbo.DimCalendar (
    DateId INT PRIMARY KEY,
    [Date] DATE NOT NULL,
    [Day] INT NOT NULL,
    [Month] INT NOT NULL,
    [Year] INT NOT NULL
);

-- Dimension Projet
CREATE TABLE dbo.DimProject (
    ProjectId INT IDENTITY(1,1) PRIMARY KEY,
    ProjectName NVARCHAR(255) NOT NULL
);

-- Dimension Utilisateur
CREATE TABLE dbo.DimUser (
    UserId INT IDENTITY(1,1) PRIMARY KEY,
    Username NVARCHAR(100) NOT NULL
);

-- Dimension Priorité
CREATE TABLE dbo.DimPriority (
    PriorityId INT IDENTITY(1,1) PRIMARY KEY,
    PriorityName NVARCHAR(50) NOT NULL
);

-- Dimension Sévérité
CREATE TABLE dbo.DimSeverity (
    SeverityId INT IDENTITY(1,1) PRIMARY KEY,
    SeverityName NVARCHAR(50) NOT NULL
);

-- Dimension Reproductibilité
CREATE TABLE dbo.DimReproducibility (
    ReproducibilityId INT IDENTITY(1,1) PRIMARY KEY,
    ReproducibilityName NVARCHAR(100) NOT NULL
);

-- Dimension Version
CREATE TABLE dbo.DimVersion (
    VersionId INT IDENTITY(1,1) PRIMARY KEY,
    VersionName NVARCHAR(100) NOT NULL
);

-- Dimension Catégorie
CREATE TABLE dbo.DimCategory (
    CategoryId INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName NVARCHAR(100) NOT NULL
);

-- Dimension OS 
CREATE TABLE dbo.DimOs (
    OsId INT IDENTITY(1,1) PRIMARY KEY,
    OsPlatform NVARCHAR(100) NOT NULL,
    OsName NVARCHAR(100) NOT NULL,
    OsVersion NVARCHAR (100) NOT NULL
);

-- Dimension Statut
-- CORRIGÉ : Ajout de IDENTITY(1,1) qui manquait
CREATE TABLE dbo.DimStatus (
    StatusId INT IDENTITY(1,1) PRIMARY KEY,
    StatusName NVARCHAR(50) NOT NULL
);

-- =============================================
-- ===           TABLE DE FAITS              ===
-- =============================================

CREATE TABLE dbo.FactBug (
    BugFactKey INT IDENTITY(1,1) PRIMARY KEY,
    BugId INT NOT NULL,
    SDC_StartDate INT FOREIGN KEY REFERENCES dbo.DimCalendar(DateId),
    SDC_EndDate INT FOREIGN KEY REFERENCES dbo.DimCalendar(DateId),
    
    [IsCurrent] BIT NOT NULL,
    [Summary] NVARCHAR(500) NOT NULL,
    
    -- Clés étrangères (FKs) vers les dimensions
    DateSubmittedId INT FOREIGN KEY REFERENCES dbo.DimCalendar(DateId),
    DateUpdatedId INT FOREIGN KEY REFERENCES dbo.DimCalendar(DateId),
    ProjectId INT FOREIGN KEY REFERENCES dbo.DimProject(ProjectId),
    
    ReporterId INT FOREIGN KEY REFERENCES dbo.DimUser(UserId),
    AssigneeId INT FOREIGN KEY REFERENCES dbo.DimUser(UserId),
    
    PriorityId INT FOREIGN KEY REFERENCES dbo.DimPriority(PriorityId),
    SeverityId INT FOREIGN KEY REFERENCES dbo.DimSeverity(SeverityId),
    ReproducibilityId INT FOREIGN KEY REFERENCES dbo.DimReproducibility(ReproducibilityId),

    ProductVersionId INT FOREIGN KEY REFERENCES dbo.DimVersion(VersionId),
    VersionFixedId INT FOREIGN KEY REFERENCES dbo.DimVersion(VersionId),

    CategoryId INT FOREIGN KEY REFERENCES dbo.DimCategory(CategoryId),
    OsId INT FOREIGN KEY REFERENCES dbo.DimOs(OsId),
    
    ViewStatusId INT FOREIGN KEY REFERENCES dbo.DimStatus(StatusId),
    StatusId INT FOREIGN KEY REFERENCES dbo.DimStatus(StatusId),
    ResolutionId INT FOREIGN KEY REFERENCES dbo.DimStatus(StatusId)

    CreatedTimestamp DATETIME,
    UpdatedTimestamp DATETIME, 
);