import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import date, datetime, timedelta # MODIFICATION : Ajout de datetime, timedelta
import re
import os
from os import getenv
from dotenv import load_dotenv
from mssql_python import connect # Je garde ta librairie
import glob # MODIFICATION : Pour lister les fichiers

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
        os.makedirs(file_path, exist_ok=True) # S'assure que le dossier existe
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

# Return data from file and loaded date of file
def get_data_from_file(file_path):
    """Charge un CSV et extrait la date de 'snapshot' depuis son nom."""
    print(f"\n--- Traitement du fichier : {file_path} ---")
    data = pd.read_csv(file_path, dtype={'Id': int}) # Assure que 'Id' est un entier
    
    # Extrait la date du nom de fichier (ex: ...2025-10-24.csv)
    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', file_path)
    if not date_match:
        print(f"AVERTISSEMENT: Impossible d'extraire la date du nom de fichier {file_path}. Utilisation de la date du jour.")
        loaded_date = date.today()
    else:
        loaded_date = date(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)))

    print(f"Date du snapshot (SDC_StartDate) : {loaded_date}")
    return data, loaded_date

# =============================================================================
# FONCTIONS DE TRANSFORMATION (T)
# =============================================================================

def clean_data(data):
    """Nettoie le DataFrame."""
    
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
            # Remplir les colonnes non-objet (si nécessaire) avec une valeur par défaut
            # ex: data[col] = data[col].fillna(0)
    
    # Nettoyage des chaînes de caractères
    column_to_lower = ['Priority', 'Severity', 'Reproducibility', 'Category', 'Status', 'Resolution', 'View Status']
    for col in column_to_lower:
        if col in data.columns:
            data[col] = data[col].str.lower()

    # Gestion des dates
    date_columns = ['Date Submitted', 'Updated']
    for col in date_columns:
        data[col] = pd.to_datetime(data[col], errors='coerce') # 'coerce' met NaT si erreur

    # Remove duplicates (conserve la première occurrence)
    data = data.drop_duplicates()

    return data

# Rename and add columns to get the history of bugs and evolution over time
def prepare_data_for_sdc2(data, loaded_date):
    """Prépare le DataFrame pour le chargement SCD2."""
    
    # Renomme pour correspondre au chargement de faits
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
    
    data['SDC_StartDate'] = pd.to_datetime(loaded_date) # Date du snapshot
    data['IsCurrent'] = True
    data['SDC_EndDate'] = None # pd.NaT ou None pour la DB

    return data

# =============================================================================
# FONCTIONS DE CHARGEMENT (L)
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

### MODIFICATION MAJEURE : Logique de chargement incrémental ###
def load_simple_dimension_incremental(db_connector, data_list, table_name, id_col, name_col, mapping):
    """
    Charge une dimension simple de manière incrémentale.
    Met à jour et retourne le dictionnaire de mapping.
    """
    print(f"Chargement incrémental de {table_name}...")
    cursor = db_connector.cursor()
    
    # 1. Obtenir les valeurs uniques du nouveau fichier
    unique_values = [str(val) for val in pd.unique(data_list) if pd.notna(val)]
    
    # 2. Trouver le max_id actuel dans le mapping
    max_id = max(mapping.values()) if mapping else 0
    if 0 not in mapping.values(): # Assurer que 'Unknown' existe
        mapping['Unknown'] = 0
        try:
            cursor.execute(f"INSERT INTO {table_name} ({id_col}, {name_col}) VALUES (0, 'Unknown')")
            db_connector.commit()
        except Exception:
            db_connector.rollback() # Existe probablement déjà, c'est OK
            
    # 3. Préparer les nouvelles données à insérer
    data_to_insert = []
    current_id = max_id + 1
    
    for val in unique_values:
        if val not in mapping: # Si la valeur est NOUVELLE
            data_to_insert.append((current_id, val))
            mapping[val] = current_id # L'ajouter au mapping
            current_id += 1
            
    # 4. Insérer uniquement les nouvelles données
    if data_to_insert:
        try:
            query = f"INSERT INTO {table_name} ({id_col}, {name_col}) VALUES (?, ?)"
            cursor.executemany(query, data_to_insert)
            db_connector.commit()
            print(f"-> Succès : {len(data_to_insert)} NOUVEAUX enregistrements chargés dans {table_name}.")
        except Exception as e:
            print(f"ERREUR lors du chargement de {table_name}: {e}")
            db_connector.rollback()
    else:
        print(f"-> Aucune nouvelle donnée pour {table_name}.")
        
    return mapping

def load_dimensions(data, db_connector, mappings):
    """
    Charge toutes les dimensions de manière incrémentale.
    'mappings' est un dictionnaire persistant qui est mis à jour.
    """
    print("\n--- Chargement des dimensions ---")
    
    # S'assurer que les sous-dictionnaires de mapping existent
    dim_simple_list = ['Project', 'User', 'Priority', 'Severity', 'Reproducibility', 'Version', 'Category', 'Status']
    for key in dim_simple_list + ['Os', 'Calendar']:
        if key not in mappings:
            mappings[key] = {}

    # DimProject
    unique_projects = data['ProjectName'].unique()
    mappings['Project'] = load_simple_dimension_incremental(
        db_connector, unique_projects, 'DimProject', 'ProjectId', 'ProjectName', mappings['Project']
    )

    # DimUser
    unique_report_user = data['ReporterName'].unique()
    unique_assignee_user = data['AssigneeName'].unique()
    all_unique_users = pd.unique(np.concatenate((unique_report_user, unique_assignee_user)))
    mappings['User'] = load_simple_dimension_incremental(
        db_connector, all_unique_users, 'DimUser', 'UserId', 'Username', mappings['User']
    )
    
    # ... Autres dimensions simples ...
    mappings['Priority'] = load_simple_dimension_incremental(
        db_connector, data['PriorityName'].unique(), 'DimPriority', 'PriorityId', 'PriorityName', mappings['Priority']
    )
    mappings['Severity'] = load_simple_dimension_incremental(
        db_connector, data['SeverityName'].unique(), 'DimSeverity', 'SeverityId', 'SeverityName', mappings['Severity']
    )
    mappings['Reproducibility'] = load_simple_dimension_incremental(
        db_connector, data['ReproducibilityName'].unique(), 'DimReproducibility', 'ReproducibilityId', 'ReproducibilityName', mappings['Reproducibility']
    )
    mappings['Category'] = load_simple_dimension_incremental(
        db_connector, data['CategoryName'].unique(), 'DimCategory', 'CategoryId', 'CategoryName', mappings['Category']
    )
    
    # DimVersion
    unique_product_versions = data['ProductVersionName'].unique()
    unique_fixed_versions = data['VersionFixedName'].unique()
    all_unique_versions = pd.unique(np.concatenate((unique_product_versions, unique_fixed_versions)))
    mappings['Version'] = load_simple_dimension_incremental(
        db_connector, all_unique_versions, 'DimVersion', 'VersionId', 'VersionName', mappings['Version']
    )

    # DimStatus
    unique_view_statuses = data['ViewStatusName'].unique()
    unique_resolution_statuses = data['ResolutionName'].unique()
    unique_statuses = data['StatusName'].unique()
    all_unique_statuses = pd.unique(np.concatenate((
        unique_view_statuses, unique_resolution_statuses, unique_statuses
    )))
    mappings['Status'] = load_simple_dimension_incremental(
        db_connector, all_unique_statuses, 'DimStatus', 'StatusId', 'StatusName', mappings['Status']
    )

    # --- Dimensions complexes (Incrémental) ---

    # DimOs (Dimension composite)
    print("Chargement incrémental de DimOs...")
    cursor = db_connector.cursor()
    os_mapping = mappings['Os']
    if not os_mapping: # Première exécution
        os_mapping[('Unknown', 'Unknown', 'Unknown')] = 0
        try:
            cursor.execute("INSERT INTO DimOs (OsId, OsPlatform, OsName, OsVersion) VALUES (0, 'Unknown', 'Unknown', 'Unknown')")
            db_connector.commit()
        except Exception: 
            db_connector.rollback()
    
    max_id = max(os_mapping.values()) if os_mapping else 0
    unique_os_combinations = data[['Platform', 'OS', 'OS Version']].drop_duplicates()
    os_data_to_insert = []
    current_id = max_id + 1
    
    for row in unique_os_combinations.itertuples(index=False):
        p, n, v = str(row.Platform), str(row.OS), str(row.OS_Version)
        if (p, n, v) not in os_mapping:
            os_data_to_insert.append((current_id, p, n, v))
            os_mapping[(p, n, v)] = current_id
            current_id += 1
            
    if os_data_to_insert:
        try:
            query = "INSERT INTO DimOs (OsId, OsPlatform, OsName, OsVersion) VALUES (?, ?, ?, ?)"
            cursor.executemany(query, os_data_to_insert)
            db_connector.commit()
            print(f"-> Succès : {len(os_data_to_insert)} NOUVEAUX enregistrements chargés dans DimOs.")
        except Exception as e:
            print(f"ERREUR lors du chargement de DimOs: {e}")
            db_connector.rollback()
    else:
        print("-> Aucune nouvelle donnée pour DimOs.")
    mappings['Os'] = os_mapping


    # DimCalendar (Incrémental)
    print("Chargement incrémental de DimCalendar...")
    cal_mapping = mappings['Calendar'] # c'est un set() des DateId existants
    if not cal_mapping:
        cal_mapping = {0} # ID 0 pour 'Unknown'
        try:
            cursor.execute("INSERT INTO DimCalendar (DateId, [Date], [Day], [Month], [Year]) VALUES (0, '1900-01-01', 1, 1, 1900)")
            db_connector.commit()
        except Exception:
            db_connector.rollback()
            
    date_submitted = pd.to_datetime(data['DateSubmitted'], errors='coerce')
    date_updated = pd.to_datetime(data['DateUpdated'], errors='coerce')
    
    all_dates = pd.unique(np.concatenate((
        date_submitted.dropna(),
        date_updated.dropna(),
        pd.to_datetime(data['SDC_StartDate'].dropna()) # Inclure les dates de snapshot
    )))
    
    unique_dates_df = pd.DataFrame(all_dates, columns=['Date']).drop_duplicates()
    unique_dates_df['DateId'] = unique_dates_df['Date'].dt.strftime('%Y%m%d').astype(int)
    
    # Filtrer les dates qui sont DÉJÀ dans le mapping
    new_dates_df = unique_dates_df[~unique_dates_df['DateId'].isin(cal_mapping)]
    
    if not new_dates_df.empty:
        new_dates_df['Day'] = new_dates_df['Date'].dt.day
        new_dates_df['Month'] = new_dates_df['Date'].dt.month
        new_dates_df['Year'] = new_dates_df['Date'].dt.year
        
        calendar_data_to_insert = [
            (row.DateId, row.Date.date(), row.Day, row.Month, row.Year)
            for row in new_dates_df.itertuples(index=False)
        ]
        
        try:
            query = "INSERT INTO DimCalendar (DateId, [Date], [Day], [Month], [Year]) VALUES (?, ?, ?, ?, ?)"
            cursor.executemany(query, calendar_data_to_insert)
            db_connector.commit()
            # Mettre à jour le set de mapping
            cal_mapping.update(new_dates_df['DateId'].tolist())
            print(f"-> Succès : {len(calendar_data_to_insert)} NOUVELLES dates chargées dans DimCalendar.")
        except Exception as e:
            print(f"ERREUR lors du chargement de DimCalendar: {e}")
            db_connector.rollback()
    else:
        print("-> Aucune nouvelle date pour DimCalendar.")
        
    mappings['Calendar'] = cal_mapping
    mappings['Calendar_Unknown_Id'] = 0 # ID fixe pour 'Unknown'

    print("--- Chargement des dimensions terminé. ---")
    return mappings


def load_fact_snapshot_scd2(data, db_connector, mappings):
    """
    Charge un snapshot de faits en utilisant la logique SCD Type 2.
    1. FERME les enregistrements courants qui vont être mis à jour.
    2. INSÈRE les nouveaux enregistrements (marqués comme courants).
    """
    print("\n--- Démarrage du chargement SCD2 de FactBug ---")
    
    # 1. Transformer le DataFrame en table de faits avec les clés de substitution
    fact_table = pd.DataFrame()
    unknown_date_id = mappings.get('Calendar_Unknown_Id', 0)
    
    # Mapper les noms aux IDs, utiliser 0 ('Unknown') si non trouvé
    fact_table['BugId'] = data['BugId']
    fact_table['ProjectId'] = data['ProjectName'].map(mappings['Project']).fillna(0).astype(int)
    fact_table['ReporterId'] = data['ReporterName'].map(mappings['User']).fillna(0).astype(int)
    fact_table['AssigneeId'] = data['AssigneeName'].map(mappings['User']).fillna(0).astype(int)
    fact_table['PriorityId'] = data['PriorityName'].map(mappings['Priority']).fillna(0).astype(int)
    fact_table['SeverityId'] = data['SeverityName'].map(mappings['Severity']).fillna(0).astype(int)
    fact_table['ReproducibilityId'] = data['ReproducibilityName'].map(mappings['Reproducibility']).fillna(0).astype(int)
    fact_table['ProductVersionId'] = data['ProductVersionName'].map(mappings['Version']).fillna(0).astype(int)
    fact_table['VersionFixedId'] = data['VersionFixedName'].map(mappings['Version']).fillna(0).astype(int)
    fact_table['CategoryId'] = data['CategoryName'].map(mappings['Category']).fillna(0).astype(int)
    fact_table['ViewStatusId'] = data['ViewStatusName'].map(mappings['Status']).fillna(0).astype(int)
    fact_table['StatusId'] = data['StatusName'].map(mappings['Status']).fillna(0).astype(int)
    fact_table['ResolutionId'] = data['ResolutionName'].map(mappings['Status']).fillna(0).astype(int)
    
    # Mappage de la dimension composite (DimOs)
    unknown_os_id = mappings['Os'].get(('Unknown', 'Unknown', 'Unknown'), 0)
    os_tuples = list(zip(
        data['Platform'].astype(str).fillna('Unknown'), 
        data['OS'].astype(str).fillna('Unknown'), 
        data['OS Version'].astype(str).fillna('Unknown')
    ))
    fact_table['OsId'] = [mappings['Os'].get(t, unknown_os_id) for t in os_tuples]

    # Mappage des dates (DimCalendar)
    fact_table['DateSubmittedId'] = data['DateSubmitted'].dt.strftime('%Y%m%d').fillna(unknown_date_id).astype(int)
    fact_table['DateUpdatedId'] = data['DateUpdated'].dt.strftime('%Y%m%d').fillna(unknown_date_id).astype(int)
    
    # Colonnes SCD et mesures
    fact_table['SDC_StartDate'] = data['SDC_StartDate'].dt.strftime('%Y%m%d').fillna(unknown_date_id).astype(int)
    fact_table['SDC_EndDate'] = None # Sera NULL
    fact_table['IsCurrent'] = 1 # True
    fact_table['Summary'] = data['Summary']

    
    cursor = db_connector.cursor()
    try:
        # --- ÉTAPE 1: FERMER les anciens enregistrements ---
        # Obtenir la date de début de ce snapshot (elle est la même pour toutes les lignes)
        sdc_start_date_obj = data['SDC_StartDate'].iloc[0]
        # La date de fin est la veille
        sdc_end_date_obj = sdc_start_date_obj - timedelta(days=1)
        sdc_end_date_id = int(sdc_end_date_obj.strftime('%Y%m%d'))
        
        # Obtenir la liste des BugId de ce snapshot
        bug_ids_in_snapshot = tuple(fact_table['BugId'].unique())
        
        if bug_ids_in_snapshot: # S'assurer que le tuple n'est pas vide
            print(f"Fermeture des anciens enregistrements pour {len(bug_ids_in_snapshot)} BugIds...")
            
            # Formatter la liste des IDs pour la clause IN
            # Note : 'mssql_python' utilise des '?'
            placeholders = ', '.join('?' for _ in bug_ids_in_snapshot)
            
            query_update = f"""
            UPDATE FactBug
            SET IsCurrent = 0, SDC_EndDate = ?
            WHERE BugId IN ({placeholders}) AND IsCurrent = 1
            """
            
            # Paramètres : d'abord la date de fin, puis tous les BugIds
            params = [sdc_end_date_id] + list(bug_ids_in_snapshot)
            
            cursor.execute(query_update, params)
            print(f"{cursor.rowcount} anciens enregistrements fermés.")

        # --- ÉTAPE 2: INSÉRER les nouveaux enregistrements ---
        print(f"Insertion de {len(fact_table)} nouveaux enregistrements (snapshot)...")
        
        fact_columns_order = [
            'BugId', 'SDC_StartDate', 'SDC_EndDate', 'IsCurrent', 'Summary',
            'DateSubmittedId', 'DateUpdatedId', 'ProjectId', 'ReporterId',
            'AssigneeId', 'PriorityId', 'SeverityId', 'ReproducibilityId',
            'ProductVersionId', 'VersionFixedId', 'CategoryId', 'OsId',
            'ViewStatusId', 'StatusId', 'ResolutionId'
        ]
        
        # S'assurer que les NaT/None sont bien 'None' pour la DB
        data_to_insert = [
            tuple(None if pd.isna(val) else val for val in row) 
            for row in fact_table[fact_columns_order].itertuples(index=False)
        ]
        
        query_insert = """
        INSERT INTO FactBug (
            BugId, SDC_StartDate, SDC_EndDate, [IsCurrent], [Summary],
            DateSubmittedId, DateUpdatedId, ProjectId, ReporterId,
            AssigneeId, PriorityId, SeverityId, ReproducibilityId,
            ProductVersionId, VersionFixedId, CategoryId, OsId,
            ViewStatusId, StatusId, ResolutionId
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.executemany(query_insert, data_to_insert)
        db_connector.commit()
        print(f"-> Succès : {len(data_to_insert)} enregistrements insérés dans FactBug.")
        
    except Exception as e:
        print(f"ERREUR lors du chargement SCD2 de FactBug: {e}")
        db_connector.rollback()
        
    print("--- Chargement de la table de faits terminé. ---")


# =============================================================================
# ORCHESTRATION DE L'ETL (Main)
# =============================================================================

def main():
    """
    Fonction principale pour orchestrer l'ensemble du processus ETL.
    """
    URL_SOURCE = 'http://teachingse.hevs.ch/csvFiles/'
    CHEMIN_DATA = './data/'

    db_connector = None
    try:
        # 1. Extraire (Télécharger les nouveaux fichiers)
        get_csv_from_url(URL_SOURCE, CHEMIN_DATA)
        
        # 2. Lister les fichiers à traiter, triés par date
        csv_files = sorted(glob.glob(os.path.join(CHEMIN_DATA, 'scribus-dump-*.csv')))
        if not csv_files:
            print("Aucun fichier local à traiter.")
            return

        # 3. Connexion DB et création du schéma (une seule fois)
        db_connector = connect_to_db()

        # 4. Charger les mappings existants depuis la DB (pour l'incrémental)
        print("Chargement des mappings de dimensions existants depuis la DB...")
        mappings = {}
        cursor = db_connector.cursor()

        mappings['Project'] = {name: id for name, id in cursor.execute("SELECT ProjectName, ProjectId FROM DimProject")}
        mappings['User'] = {name: id for name, id in cursor.execute("SELECT Username, UserId FROM DimUser")}
        mappings['Priority'] = {name: id for name, id in cursor.execute("SELECT PriorityName, PriorityId FROM DimPriority")}
        mappings['Severity'] = {name: id for name, id in cursor.execute("SELECT SeverityName, SeverityId FROM DimSeverity")}
        mappings['Reproducibility'] = {name: id for name, id in cursor.execute("SELECT ReproducibilityName, ReproducibilityId FROM DimReproducibility")}
        mappings['Version'] = {name: id for name, id in cursor.execute("SELECT VersionName, VersionId FROM DimVersion")}
        mappings['Category'] = {name: id for name, id in cursor.execute("SELECT CategoryName, CategoryId FROM DimCategory")}
        mappings['Status'] = {name: id for name, id in cursor.execute("SELECT StatusName, StatusId FROM DimStatus")}
        mappings['Os'] = {(p, n, v): id for p, n, v, id in cursor.execute("SELECT OsPlatform, OsName, OsVersion, OsId FROM DimOs")}
        mappings['Calendar'] = {id[0] for id in cursor.execute("SELECT DateId FROM DimCalendar")}
        print("Mappings existants chargés.")

        # 5. Boucle de traitement pour chaque fichier (T et L)
        for file_path in csv_files:
            # (T) Obtenir les données brutes du fichier
            data, loaded_date = get_data_from_file(file_path)
            
            # (T) Nettoyer les données
            data = clean_data(data)
            
            # (T) Préparer pour le SCD2 (renommage, ajout de colonnes SDC)
            data = prepare_data_for_sdc2(data, loaded_date)

            # (L) Charger/Mettre à jour les dimensions
            # On passe les 'mappings' pour qu'ils soient mis à jour
            mappings = load_dimensions(data, db_connector, mappings)
            
            # (L) Charger le snapshot de faits
            load_fact_snapshot_scd2(data, db_connector, mappings)

        print("\n=== Processus ETL terminé avec succès pour tous les fichiers. ===")

    except Exception as e:
        print(f"ERREUR MAJEURE dans le processus ETL: {e}")
    finally:
        if db_connector:
            db_connector.close()
            print("Connexion à la base de données fermée.")

if __name__ == "__main__":
    main()