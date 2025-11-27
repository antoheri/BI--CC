import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import date, timedelta
import re
import os
from os import getenv
from dotenv import load_dotenv
from mssql_python import connect # Garde ta librairie
import glob
import numpy as np

# =============================================================================
# FONCTIONS D'EXTRACTION (E)
# =============================================================================

def get_csv_from_url(URL, file_path='./data/'):
    """Télécharge les fichiers CSV qui ne sont pas déjà présents localement."""
    print(f"Vérification des nouveaux fichiers CSV depuis {URL}...")
    try:
        soup = BeautifulSoup(requests.get(URL).text, features="html.parser")
        table = soup.table
        if not table:
            print("Aucune table trouvée à l'URL.")
            return

        links = table.find_all("a")
        os.makedirs(file_path, exist_ok=True)
        downloaded_files = os.listdir(file_path)
        files_to_download = []

        for l in links:
            file_name = l.get('href')
            if "scribus" in file_name and file_name.endswith('.csv'):
                if file_name not in downloaded_files:
                    files_to_download.append(file_name)

        if not files_to_download:
            print("Aucun nouveau fichier à télécharger.")
            return

        print(f"{len(files_to_download)} nouveau(x) fichier(s) à télécharger.")
        for file_name in files_to_download:
            try:
                response = requests.get(URL + file_name)
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))

                with open(os.path.join(file_path, file_name), 'wb') as file, \
                    tqdm(desc=file_name, total=total_size, unit='iB', unit_scale=True) as progress_bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        size = file.write(chunk)
                        progress_bar.update(size)
            except requests.exceptions.RequestException as e:
                print(f"Erreur lors du téléchargement du fichier {file_name} : {e}")
                
    except requests.exceptions.RequestException as e:
        print(f"Erreur d'accès à l'URL {URL} : {e}")

def get_data_from_file(file_path):
    """Charge un CSV et extrait la date de 'snapshot' depuis son nom."""
    print(f"\n--- Traitement du fichier : {file_path} ---")
    data = pd.read_csv(file_path, dtype={'Id': int})
    
    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', file_path)
    if not date_match:
        print(f"AVERTISSEMENT: Impossible d'extraire la date. Utilisation de la date du jour.")
        loaded_date = date.today()
    else:
        loaded_date = date(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)))

    print(f"Date du snapshot (SDC_StartDate) : {loaded_date}")
    return data, loaded_date

# =============================================================================
# FONCTIONS DE TRANSFORMATION (T)
# =============================================================================

def clean_data(data):
    """Nettoie le DataFrame source."""
    
    cols_to_fill = [
        'Project', 'Reporter', 'Assigned To', 'Priority', 'Severity', 
        'Reproducibility', 'Product Version', 'Fixed in Version', 'Category',
        'OS', 'OS Version', 'Platform', 'View Status', 'Status', 'Resolution',
        'Summary'
    ]
    for col in cols_to_fill:
        if col in data.columns:
            if data[col].dtype == 'object':
                data[col] = data[col].fillna('Unknown')
    
    column_to_lower = ['Priority', 'Severity', 'Reproducibility', 'Category', 'Status', 'Resolution', 'View Status']
    for col in column_to_lower:
        if col in data.columns:
            data[col] = data[col].str.lower()

    date_columns = ['Date Submitted', 'Updated']
    for col in date_columns:
        data[col] = pd.to_datetime(data[col], errors='coerce')

    data = data.drop_duplicates()
    return data

def prepare_data_for_staging(data, loaded_date):
    """Prépare le DataFrame pour le chargement en table de staging."""
    
    # Renomme les colonnes pour correspondre aux noms attendus par le staging
    data.rename(columns={
        'Id': 'BugId',
        'Date Submitted': 'DateSubmitted',
        'Updated': 'DateUpdated',
        'Project': 'ProjectName',
        'Reporter': 'ReporterName',
        'Assigned To': 'AssigneeName',
        'Priority': 'PriorityName',
        'Severity': 'SeverityName',
        'Reproducibility': 'ReproducibilityName',
        'Product Version': 'ProductVersionName',
        'Fixed in Version': 'VersionFixedName',
        'Category': 'CategoryName',
        'View Status': 'ViewStatusName',
        'Status': 'StatusName',
        'Resolution': 'ResolutionName'
    }, inplace=True)
    
    data['SDC_StartDate'] = pd.to_datetime(loaded_date)
    return data

# =============================================================================
# FONCTIONS DE CHARGEMENT (L) - APPROCHE STAGING
# =============================================================================

def connect_to_db():
    """Charge les variables d'env et se connecte à la DB."""
    load_dotenv()
    conn_string = getenv("SQL_CONNECTION_STRING")
    if not conn_string:
        raise ValueError("La variable d'environnement SQL_CONNECTION_STRING n'est pas définie.")
    print("Connexion à la base de données...")
    conn = connect(conn_string)
    print("Connexion établie.")
    return conn

def _merge_simple_dimension(cursor, stage_table, target_table, column_name, data_list, nvarchar_size=255):
    """
    Fonction helper pour charger une dimension simple (ID, Nom) via staging.
    NOTE : Suppose que la colonne ID de la table cible est IDENTITY(1,1).
    """
    print(f"-> Staging {target_table}...")
    unique_values = [(str(val),) for val in pd.unique(data_list) if pd.notna(val) and str(val) != 'Unknown']
    if not unique_values:
        print(f"-> Aucune nouvelle donnée pour {target_table}.")
        return

    try:
        # Créer la table de staging temporaire (correction de la parenthèse manquante)
        cursor.execute(f"CREATE TABLE #{stage_table} ({column_name} NVARCHAR({nvarchar_size}))")
        
        # Insérer les données uniques
        cursor.executemany(f"INSERT INTO #{stage_table} ({column_name}) VALUES (?)", unique_values)
        
        # Utiliser MERGE pour insérer uniquement les nouveaux
        cursor.execute(f"""
            MERGE INTO dbo.{target_table} AS T
            USING (SELECT DISTINCT {column_name} FROM #{stage_table}) AS S
            ON T.{column_name} = S.{column_name}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({column_name}) VALUES (S.{column_name});
        """)
        
        # Nettoyer
        cursor.execute(f"DROP TABLE #{stage_table}")
        
    except Exception as e:
        print(f"ERREUR pendant le staging de {target_table}: {e}")
        cursor.execute(f"DROP TABLE IF EXISTS #{stage_table}") # Nettoyage en cas d'erreur
        raise e


def update_dimensions_staging(data, db_connector):
    """
    Met à jour TOUTES les dimensions en utilisant des tables de staging et des requêtes MERGE.
    """
    print("Mise à jour des dimensions via Staging...")
    cursor = db_connector.cursor()
    
    try:
        # --- Dimensions Simples ---
        _merge_simple_dimension(cursor, "StageProject", "DimProject", "ProjectName", 
                                data['ProjectName'].unique(), 255)
        
        all_users = pd.unique(np.concatenate((data['ReporterName'].unique(), data['AssigneeName'].unique())))
        _merge_simple_dimension(cursor, "StageUser", "DimUser", "Username", 
                                all_users, 100)
        
        _merge_simple_dimension(cursor, "StagePriority", "DimPriority", "PriorityName", 
                                data['PriorityName'].unique(), 50)
                                
        _merge_simple_dimension(cursor, "StageSeverity", "DimSeverity", "SeverityName", 
                                data['SeverityName'].unique(), 50)
        
        _merge_simple_dimension(cursor, "StageRepro", "DimReproducibility", "ReproducibilityName", 
                                data['ReproducibilityName'].unique(), 100)
        
        all_versions = pd.unique(np.concatenate((data['ProductVersionName'].unique(), data['VersionFixedName'].unique())))
        _merge_simple_dimension(cursor, "StageVersion", "DimVersion", "VersionName", 
                                all_versions, 100)
        
        _merge_simple_dimension(cursor, "StageCategory", "DimCategory", "CategoryName", 
                                data['CategoryName'].unique(), 100)
        
        all_statuses = pd.unique(np.concatenate((
            data['ViewStatusName'].unique(), data['StatusName'].unique(), data['ResolutionName'].unique()
        )))
        _merge_simple_dimension(cursor, "StageStatus", "DimStatus", "StatusName", 
                                all_statuses, 50)

        # --- Dimension Composite : DimOs ---
        print("-> Staging DimOs...")
        unique_os = data[['Platform', 'OS', 'OS Version']].drop_duplicates()
        unique_values = [tuple(x) for x in unique_os.values if not all(pd.isna(x))]
        
        if unique_values:
            cursor.execute("CREATE TABLE #StageOs (OsPlatform NVARCHAR(100), OsName NVARCHAR(100), OsVersion NVARCHAR(100))")
            cursor.executemany("INSERT INTO #StageOs (OsPlatform, OsName, OsVersion) VALUES (?, ?, ?)", unique_values)
            cursor.execute("""
                MERGE INTO dbo.DimOs AS T
                USING (SELECT DISTINCT OsPlatform, OsName, OsVersion FROM #StageOs) AS S
                ON (T.OsPlatform = S.OsPlatform AND T.OsName = S.OsName AND T.OsVersion = S.OsVersion)
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (OsPlatform, OsName, OsVersion) 
                    VALUES (S.OsPlatform, S.OsName, S.OsVersion);
            """)
            cursor.execute("DROP TABLE #StageOs")

        # --- Dimension Spéciale : DimCalendar ---
        print("-> Staging DimCalendar...")
        # Calcule le SDC_EndDate (la veille) MAINTENANT
        # pour s'assurer qu'il est ajouté au calendrier.
        sdc_start_date_obj = pd.to_datetime(data['SDC_StartDate'].iloc[0])
        sdc_end_date_obj = sdc_start_date_obj - timedelta(days=1)

      # Crée un array numpy de datetime64, le même type que les .values des autres
        sdc_end_date_array = pd.to_datetime([sdc_end_date_obj]).values 
        
        # Concatène les .values de toutes les sources de date
        all_dates_pd = pd.unique(np.concatenate((
            pd.to_datetime(data['DateSubmitted'], errors='coerce').dropna().values,
            pd.to_datetime(data['DateUpdated'], errors='coerce').dropna().values,
            pd.to_datetime(data['SDC_StartDate'], errors='coerce').dropna().values,
            sdc_end_date_array # <-- Utilise l'array numpy robuste
        )))
        
        if len(all_dates_pd) > 0:
            cal_df = pd.DataFrame(all_dates_pd, columns=['Date']).drop_duplicates()
            cal_df['DateId'] = cal_df['Date'].dt.strftime('%Y%m%d').astype(int)
            cal_df['Day'] = cal_df['Date'].dt.day
            cal_df['Month'] = cal_df['Date'].dt.month
            cal_df['Year'] = cal_df['Date'].dt.year
            cal_df['Date_SQL'] = cal_df['Date'].dt.date
            
            cal_values = [tuple(x) for x in cal_df[['DateId', 'Date_SQL', 'Day', 'Month', 'Year']].values]

            cursor.execute("CREATE TABLE #StageCalendar (DateId INT PRIMARY KEY, [Date] DATE, [Day] INT, [Month] INT, [Year] INT)")
            cursor.executemany("INSERT INTO #StageCalendar (DateId, [Date], [Day], [Month], [Year]) VALUES (?, ?, ?, ?, ?)", cal_values)
            cursor.execute("""
                MERGE INTO dbo.DimCalendar AS T
                USING #StageCalendar AS S
                ON T.DateId = S.DateId
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (DateId, [Date], [Day], [Month], [Year])
                    VALUES (S.DateId, S.[Date], S.[Day], S.[Month], S.[Year]);
            """)
            cursor.execute("DROP TABLE #StageCalendar")

        db_connector.commit()
        print("Dimensions mises à jour avec succès.")
        
    except Exception as e:
        print(f"ERREUR lors de la mise à jour des dimensions : {e}")
        db_connector.rollback()
        raise e


def load_fact_snapshot_scd2_staging(data, db_connector):
    """
    Charge le snapshot de faits en utilisant une table de staging
    et une seule requête INSERT ... SELECT ... JOIN.
    """
    print(f"Chargement du snapshot de faits via Staging...")
    cursor = db_connector.cursor()
    
    # 1. Préparer les données pour l'insertion en table de staging
    data['DateSubmitted_SQL'] = data['DateSubmitted'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
    data['DateUpdated_SQL'] = data['DateUpdated'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None)
    data['SDC_StartDate_SQL'] = data['SDC_StartDate'].dt.strftime('%Y-%m-%d')
    
    # Remplacer les NaN par None (pour NULL en SQL)
    data = data.where(pd.notna(data), None)

    # 2. Créer la table de staging #StageFact
    try:
        cursor.execute("""
        CREATE TABLE #StageFact (
            BugId INT PRIMARY KEY,
            ProjectName NVARCHAR(255),
            ReporterName NVARCHAR(100),
            AssigneeName NVARCHAR(100),
            PriorityName NVARCHAR(50),
            SeverityName NVARCHAR(50),
            ReproducibilityName NVARCHAR(100),
            ProductVersionName NVARCHAR(100),
            VersionFixedName NVARCHAR(100),
            CategoryName NVARCHAR(100),
            Platform NVARCHAR(100),
            OS NVARCHAR(100),
            OS_Version NVARCHAR(100),
            ViewStatusName NVARCHAR(50),
            StatusName NVARCHAR(50),
            ResolutionName NVARCHAR(50),
            Summary NVARCHAR(500),
            DateSubmitted DATETIME,
            DateUpdated DATETIME,
            SDC_StartDate DATE
        )
        """)

        # 3. Insérer les données du DataFrame dans #StageFact
        data_to_insert = list(data[[
            'BugId', 'ProjectName', 'ReporterName', 'AssigneeName', 'PriorityName',
            'SeverityName', 'ReproducibilityName', 'ProductVersionName', 'VersionFixedName',
            'CategoryName', 'Platform', 'OS', 'OS Version', 'ViewStatusName',
            'StatusName', 'ResolutionName', 'Summary', 
            'DateSubmitted_SQL', 'DateUpdated_SQL', 'SDC_StartDate_SQL'
        ]].itertuples(index=False, name=None))
        
        insert_query = "INSERT INTO #StageFact VALUES (" + "?,"*19 + "?)"
        cursor.executemany(insert_query, data_to_insert)

        # 4. Gérer le SCD2 (Fermer les anciens enregistrements)
        sdc_start_date_obj = data['SDC_StartDate'].iloc[0]
        sdc_end_date_obj = sdc_start_date_obj - timedelta(days=1)
        sdc_end_date_id = int(sdc_end_date_obj.strftime('%Y%m%d')) # ID du calendrier

        query_update = f"""
        UPDATE dbo.FactBug
        SET IsCurrent = 0, SDC_EndDate = ?
        WHERE BugId IN (SELECT BugId FROM #StageFact) AND IsCurrent = 1
        """
        cursor.execute(query_update, (sdc_end_date_id))
        print(f"{cursor.rowcount} anciens enregistrements fermés.")

        # 5. Insérer les nouveaux enregistrements avec LOOKUP (la grosse requête)
        query_insert = """
        INSERT INTO dbo.FactBug (
            BugId, SDC_StartDate, SDC_EndDate, [IsCurrent], [Summary],
            DateSubmittedId, DateUpdatedId, ProjectId, ReporterId, AssigneeId,
            PriorityId, SeverityId, ReproducibilityId, ProductVersionId, VersionFixedId,
            CategoryId, OsId, ViewStatusId, StatusId, ResolutionId
        )
        SELECT
            sf.BugId,
            ISNULL(dc_start.DateId, 0) AS SDC_StartDate,
            NULL AS SDC_EndDate,
            1 AS IsCurrent,
            sf.Summary,
            ISNULL(dc_sub.DateId, 0) AS DateSubmittedId,
            ISNULL(dc_upd.DateId, 0) AS DateUpdatedId,
            ISNULL(p.ProjectId, 0) AS ProjectId,
            ISNULL(u_rep.UserId, 0) AS ReporterId,
            ISNULL(u_ass.UserId, 0) AS AssigneeId,
            ISNULL(pri.PriorityId, 0) AS PriorityId,
            ISNULL(sev.SeverityId, 0) AS SeverityId,
            ISNULL(rep.ReproducibilityId, 0) AS ReproducibilityId,
            ISNULL(v_prod.VersionId, 0) AS ProductVersionId,
            ISNULL(v_fix.VersionId, 0) AS VersionFixedId,
            ISNULL(cat.CategoryId, 0) AS CategoryId,
            ISNULL(os.OsId, 0) AS OsId,
            ISNULL(s_view.StatusId, 0) AS ViewStatusId,
            ISNULL(s_stat.StatusId, 0) AS StatusId,
            ISNULL(s_res.StatusId, 0) AS ResolutionId
        FROM #StageFact AS sf
        -- Jointures de lookup pour trouver les IDs
        LEFT JOIN dbo.DimProject AS p ON sf.ProjectName = p.ProjectName
        LEFT JOIN dbo.DimUser AS u_rep ON sf.ReporterName = u_rep.Username
        LEFT JOIN dbo.DimUser AS u_ass ON sf.AssigneeName = u_ass.Username
        LEFT JOIN dbo.DimPriority AS pri ON sf.PriorityName = pri.PriorityName
        LEFT JOIN dbo.DimSeverity AS sev ON sf.SeverityName = sev.SeverityName
        LEFT JOIN dbo.DimReproducibility AS rep ON sf.ReproducibilityName = rep.ReproducibilityName
        LEFT JOIN dbo.DimVersion AS v_prod ON sf.ProductVersionName = v_prod.VersionName
        LEFT JOIN dbo.DimVersion AS v_fix ON sf.VersionFixedName = v_fix.VersionName
        LEFT JOIN dbo.DimCategory AS cat ON sf.CategoryName = cat.CategoryName
        LEFT JOIN dbo.DimStatus AS s_view ON sf.ViewStatusName = s_view.StatusName
        LEFT JOIN dbo.DimStatus AS s_stat ON sf.StatusName = s_stat.StatusName
        LEFT JOIN dbo.DimStatus AS s_res ON sf.ResolutionName = s_res.StatusName
        LEFT JOIN dbo.DimOs AS os ON sf.Platform = os.OsPlatform AND sf.OS = os.OsName AND sf.OS_Version = os.OsVersion
        -- Jointures de date (nécessite DimCalendar à jour)
        LEFT JOIN dbo.DimCalendar AS dc_start ON sf.SDC_StartDate = dc_start.Date
        LEFT JOIN dbo.DimCalendar AS dc_sub ON CAST(sf.DateSubmitted AS DATE) = dc_sub.Date
        LEFT JOIN dbo.DimCalendar AS dc_upd ON CAST(sf.DateUpdated AS DATE) = dc_upd.Date;
        """
        
        cursor.execute(query_insert)
        print(f"{cursor.rowcount} nouveaux enregistrements insérés dans FactBug.")

        # 6. Nettoyage
        cursor.execute("DROP TABLE #StageFact")
        db_connector.commit()

    except Exception as e:
        print(f"ERREUR lors du chargement de FactBug via Staging: {e}")
        db_connector.rollback()
        raise e # Propage l'erreur

# =============================================================================
# ORCHESTRATION DE L'ETL (Main)
# =============================================================================
def main():
    """
    Fonction principale (logique de staging).
    """
    URL_SOURCE = 'http://teachingse.hevs.ch/csvFiles/'
    CHEMIN_DATA = './data/'

    db_connector = None
    try:
        # 1. Extraire
        get_csv_from_url(URL_SOURCE, CHEMIN_DATA)
        
        csv_files = sorted(glob.glob(os.path.join(CHEMIN_DATA, 'scribus-dump-*.csv')))
        if not csv_files:
            print("Aucun fichier local à traiter.")
            return

        # 2. Connexion DB
        db_connector = connect_to_db()

        # 3. Boucle de traitement pour chaque fichier (T et L)
        for file_path in csv_files:
            # (T) Obtenir les données brutes du fichier
            data, loaded_date = get_data_from_file(file_path)
            # (T) Nettoyer les données
            data = clean_data(data)
            # (T) Préparer pour le Staging (renommage, ajout SDC_StartDate)
            data = prepare_data_for_staging(data, loaded_date)

            print(f"--- Traitement Snapshot {loaded_date} ---")

            # (L) Étape 1 : Mettre à jour les dimensions à partir de ce snapshot
            update_dimensions_staging(data, db_connector)
            
            # (L) Étape 2 : Charger la table de faits en utilisant le staging
            load_fact_snapshot_scd2_staging(data, db_connector)

        print("\n=== Processus ETL (Staging) terminé avec succès pour tous les fichiers. ===")

    except Exception as e:
        print(f"ERREUR MAJEURE dans le processus ETL: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if db_connector:
            db_connector.close()
            print("Connexion à la base de données fermée.")

if __name__ == "__main__":
    main()