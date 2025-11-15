USE DWBugs;
GO

-- ============================================================
-- SCRIPT DE RÉPARATION : AJOUT DES IDs 0 (UNKNOWN) MANQUANTS
-- ============================================================

-- 1. DimCalendar (Pas d'Identity, insertion simple)
IF NOT EXISTS (SELECT 1 FROM dbo.DimCalendar WHERE DateId = 0)
BEGIN
    INSERT INTO dbo.DimCalendar (DateId, [Date], [Day], [Month], [Year])
    VALUES (0, '1900-01-01', 1, 1, 1900);
    PRINT 'DimCalendar: ID 0 ajouté.';
END

-- 2. DimProject
IF NOT EXISTS (SELECT 1 FROM dbo.DimProject WHERE ProjectId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimProject ON;
    INSERT INTO dbo.DimProject (ProjectId, ProjectName) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimProject OFF;
    PRINT 'DimProject: ID 0 ajouté.';
END

-- 3. DimUser (C'est celui qui plante actuellement)
IF NOT EXISTS (SELECT 1 FROM dbo.DimUser WHERE UserId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimUser ON;
    INSERT INTO dbo.DimUser (UserId, Username) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimUser OFF;
    PRINT 'DimUser: ID 0 ajouté.';
END

-- 4. DimPriority
IF NOT EXISTS (SELECT 1 FROM dbo.DimPriority WHERE PriorityId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimPriority ON;
    INSERT INTO dbo.DimPriority (PriorityId, PriorityName) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimPriority OFF;
    PRINT 'DimPriority: ID 0 ajouté.';
END

-- 5. DimSeverity
IF NOT EXISTS (SELECT 1 FROM dbo.DimSeverity WHERE SeverityId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimSeverity ON;
    INSERT INTO dbo.DimSeverity (SeverityId, SeverityName) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimSeverity OFF;
    PRINT 'DimSeverity: ID 0 ajouté.';
END

-- 6. DimReproducibility
IF NOT EXISTS (SELECT 1 FROM dbo.DimReproducibility WHERE ReproducibilityId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimReproducibility ON;
    INSERT INTO dbo.DimReproducibility (ReproducibilityId, ReproducibilityName) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimReproducibility OFF;
    PRINT 'DimReproducibility: ID 0 ajouté.';
END

-- 7. DimVersion
IF NOT EXISTS (SELECT 1 FROM dbo.DimVersion WHERE VersionId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimVersion ON;
    INSERT INTO dbo.DimVersion (VersionId, VersionName) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimVersion OFF;
    PRINT 'DimVersion: ID 0 ajouté.';
END

-- 8. DimCategory
IF NOT EXISTS (SELECT 1 FROM dbo.DimCategory WHERE CategoryId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimCategory ON;
    INSERT INTO dbo.DimCategory (CategoryId, CategoryName) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimCategory OFF;
    PRINT 'DimCategory: ID 0 ajouté.';
END

-- 9. DimOs
IF NOT EXISTS (SELECT 1 FROM dbo.DimOs WHERE OsId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimOs ON;
    INSERT INTO dbo.DimOs (OsId, OsPlatform, OsName, OsVersion) VALUES (0, 'Unknown', 'Unknown', 'Unknown');
    SET IDENTITY_INSERT dbo.DimOs OFF;
    PRINT 'DimOs: ID 0 ajouté.';
END

-- 10. DimStatus
IF NOT EXISTS (SELECT 1 FROM dbo.DimStatus WHERE StatusId = 0)
BEGIN
    SET IDENTITY_INSERT dbo.DimStatus ON;
    INSERT INTO dbo.DimStatus (StatusId, StatusName) VALUES (0, 'Unknown');
    SET IDENTITY_INSERT dbo.DimStatus OFF;
    PRINT 'DimStatus: ID 0 ajouté.';
END
GO