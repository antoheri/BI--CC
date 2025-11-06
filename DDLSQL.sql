/*
================================================================
 Script de création de la base de données (DWBugs)
 Version 2 (avec ajout de Summary et suppression de DimOs.Field)
================================================================
*/

-- Création de la base de données DWBugs
CREATE DATABASE DWBugs;
GO
USE DWBugs;

-- =============================================
-- ===         TABLES DE DIMENSIONS          ===
-- =============================================

-- Dimension Calendrier
CREATE TABLE DimCalendar (
    DateId INT PRIMARY KEY,
    [Date] DATE NOT NULL,
    [Day] INT NOT NULL,
    [Month] INT NOT NULL,
    [Year] INT NOT NULL
);

-- Dimension Projet
CREATE TABLE DimProject (
    ProjectId INT PRIMARY KEY,
    ProjectName NVARCHAR(255) NOT NULL
);

-- Dimension Utilisateur
CREATE TABLE DimUser (
    UserId INT PRIMARY KEY,
    Username NVARCHAR(100) NOT NULL
);

-- Dimension Priorité
CREATE TABLE DimPriority (
    PriorityId INT PRIMARY KEY,
    PriorityName NVARCHAR(50) NOT NULL
);

-- Dimension Sévérité
CREATE TABLE DimSeverity (
    SeverityId INT PRIMARY KEY,
    SeverityName NVARCHAR(50) NOT NULL
);

-- Dimension Reproductibilité
CREATE TABLE DimReproducibility (
    ReproducibilityId INT PRIMARY KEY,
    ReproducibilityName NVARCHAR(100) NOT NULL
);

-- Dimension Version
CREATE TABLE DimVersion (
    VersionId INT PRIMARY KEY,
    VersionName NVARCHAR(100) NOT NULL
);

-- Dimension Catégorie
CREATE TABLE DimCategory (
    CategoryId INT PRIMARY KEY,
    CategoryName NVARCHAR(100) NOT NULL
);

-- Dimension OS 
CREATE TABLE DimOs (
    OsId INT PRIMARY KEY,
    OsPlatform NVARCHAR(100) NOT NULL,
    OsName NVARCHAR(100) NOT NULL,
    OsVersion NVARCHAR (100) NOT NULL
);

-- Dimension Statut
CREATE TABLE DimStatus (
    StatusId INT PRIMARY KEY,
    StatusName NVARCHAR(50) NOT NULL
);

-- =============================================
-- ===           TABLE DE FAITS              ===
-- =============================================

CREATE TABLE FactBug (
    BugId INT PRIMARY KEY,
    
    [Summary] NVARCHAR(500) NOT NULL, -- Ajout de la colonne Summary
    
    -- Clés étrangères (FKs) vers les dimensions
    DateSubmittedId INT FOREIGN KEY REFERENCES DimCalendar(DateId),
    DateUpdatedId INT FOREIGN KEY REFERENCES DimCalendar(DateId),
    ProjectId INT FOREIGN KEY REFERENCES DimProject(ProjectId),
    
    ReporterId INT FOREIGN KEY REFERENCES DimUser(UserId),
    AssigneeId INT FOREIGN KEY REFERENCES DimUser(UserId),
    
    PriorityId INT FOREIGN KEY REFERENCES DimPriority(PriorityId),
    SeverityId INT FOREIGN KEY REFERENCES DimSeverity(SeverityId),
    ReproducibilityId INT FOREIGN KEY REFERENCES DimReproducibility(ReproducibilityId),

    ProductVersionId INT FOREIGN KEY REFERENCES DimVersion(VersionId),
    VersionFixedId INT FOREIGN KEY REFERENCES DimVersion(VersionId),

    CategoryId INT FOREIGN KEY REFERENCES DimCategory(CategoryId),
    OsId INT FOREIGN KEY REFERENCES DimOs(OsId),
    
    ViewStatusId INT FOREIGN KEY REFERENCES DimStatus(StatusId),
    StatusId INT FOREIGN KEY REFERENCES DimStatus(StatusId),
    ResolutionId INT FOREIGN KEY REFERENCES DimStatus(StatusId)
);
