def load_simple_dimension(db_connector, data_list, table_name, id_col, name_col):
    """
    Charge une dimension simple (ID, Nom) dans la base de données.
    Réserve l'ID 0 pour 'Unknown'.
    Retourne un dictionnaire de mapping {nom: id}.
    """
    print(f"Chargement de {table_name}...")
    cursor = db_connector.cursor()
    
    # 1. Obtenir les valeurs uniques, sans 'Unknown' (on le gère séparément)
    # On convertit en str pour éviter les problèmes de type
    unique_values = [str(val) for val in pd.unique(data_list) if pd.notna(val) and str(val) != 'Unknown']
    unique_values.sort() # Assure un ordre constant
    
    # 2. Préparer les données pour l'insertion
    # ID 0 est réservé pour 'Unknown'
    data_to_insert = [(0, 'Unknown')]
    data_to_insert.extend([(i + 1, name) for i, name in enumerate(unique_values)])
    
    # 3. Créer le mapping {nom: id}
    mapping = {name: i + 1 for i, name in enumerate(unique_values)}
    mapping['Unknown'] = 0
    
    # 4. Exécuter la suppression et l'insertion
    try:
        cursor.execute(f"DELETE FROM {table_name}")
        
        query = f"INSERT INTO {table_name} ({id_col}, {name_col}) VALUES (?, ?)"
        cursor.executemany(query, data_to_insert)
        
        db_connector.commit()
        print(f"-> Succès : {len(data_to_insert)} enregistrements chargés dans {table_name}.")
        return mapping
        
    except Exception as e:
        print(f"ERREUR lors du chargement de {table_name}: {e}")
        db_connector.rollback()
        return {} # Retourne un mapping vide en cas d'erreur

# =============================================================================
# FONCTION DE CHARGEMENT DES DIMENSIONS (COMPLÉTÉE)
# =============================================================================

def load_dimensions(data, db_connector):
    """
    Extrait les valeurs uniques du DataFrame et charge toutes les dimensions.
    Retourne un dictionnaire de dictionnaires de mapping.
    """
    mappings = {}
    
    # --- 1. Nettoyage des données (remplacer NaN par 'Unknown') ---
    # C'est crucial pour que les mappings fonctionnent
    cols_to_clean = [
        'Project', 'Reporter', 'Assigned To', 'Priority', 'Severity', 
        'Reproducibility', 'Product Version', 'Fixed in Version', 'Category',
        'OS', 'OS Version', 'Platform', 'View Status', 'Status', 'Resolution'
    ]
    for col in cols_to_clean:
        if col in data.columns:
            data[col] = data[col].fillna('Unknown')
        else:
            print(f"Attention : Colonne '{col}' non trouvée dans le CSV.")
            
    # --- 2. Correction des typos de l'ébauche de code ---
    # (Note : J'utilise les noms de colonnes du CSV)
    
    # DimProject
    unique_projects = data['Project'].unique()
    mappings['Project'] = load_simple_dimension(
        db_connector, unique_projects, 'DimProject', 'ProjectId', 'ProjectName'
    )

    # DimUser
    unique_report_user = data['Reporter'].unique()
    unique_assignee_user = data['Assigned To'].unique() # Corrigé
    all_unique_users = pd.unique(np.concatenate((unique_report_user, unique_assignee_user)))
    mappings['User'] = load_simple_dimension(
        db_connector, all_unique_users, 'DimUser', 'UserId', 'Username'
    )

    # DimPriority
    unique_priorities = data['Priority'].unique()
    mappings['Priority'] = load_simple_dimension(
        db_connector, unique_priorities, 'DimPriority', 'PriorityId', 'PriorityName'
    )

    # DimSeverity
    unique_severities = data['Severity'].unique()
    mappings['Severity'] = load_simple_dimension(
        db_connector, unique_severities, 'DimSeverity', 'SeverityId', 'SeverityName'
    )

    # DimReproducibility
    unique_reproducibility = data['Reproducibility'].unique()
    mappings['Reproducibility'] = load_simple_dimension(
        db_connector, unique_reproducibility, 'DimReproducibility', 'ReproducibilityId', 'ReproducibilityName'
    )

    # DimVersion
    unique_product_versions = data['Product Version'].unique()
    unique_fixed_versions = data['Fixed in Version'].unique()
    all_unique_versions = pd.unique(np.concatenate((unique_product_versions, unique_fixed_versions)))
    mappings['Version'] = load_simple_dimension(
        db_connector, all_unique_versions, 'DimVersion', 'VersionId', 'VersionName'
    )

    # DimCategory
    unique_categories = data['Category'].unique() # Corrigé 'Categories'
    mappings['Category'] = load_simple_dimension(
        db_connector, unique_categories, 'DimCategory', 'CategoryId', 'CategoryName'
    )
    
    # DimStatus
    unique_view_statuses = data['View Status'].unique()
    unique_resolution_statuses = data['Resolution'].unique()
    unique_statuses = data['Status'].unique()
    all_unique_statuses = pd.unique(np.concatenate((
        unique_view_statuses, unique_resolution_statuses, unique_statuses
    )))
    mappings['Status'] = load_simple_dimension(
        db_connector, all_unique_statuses, 'DimStatus', 'StatusId', 'StatusName'
    )

    # --- 3. Dimensions complexes (Cas spéciaux) ---

    # DimOs (Dimension composite)
    print("Chargement de DimOs...")
    try:
        # Obtenir les combinaisons uniques
        unique_os_combinations = data[['Platform', 'OS', 'OS Version']].drop_duplicates()
        
        cursor = db_connector.cursor()
        cursor.execute("DELETE FROM DimOs")
        
        os_data_to_insert = [(0, 'Unknown', 'Unknown', 'Unknown')] # ID 0
        os_mapping = {('Unknown', 'Unknown', 'Unknown'): 0}
        
        id_counter = 1
        for row in unique_os_combinations.itertuples(index=False):
            # Assurer que tout est en string
            p = str(row.Platform)
            n = str(row.OS)
            v = str(row.OS_Version)
            
            # Ne pas ré-ajouter la combinaison 'Unknown' si elle vient des données
            if (p, n, v) == ('Unknown', 'Unknown', 'Unknown'):
                continue
                
            os_data_to_insert.append((id_counter, p, n, v))
            os_mapping[(p, n, v)] = id_counter
            id_counter += 1
        
        query = "INSERT INTO DimOs (OsId, OsPlatform, OsName, OsVersion) VALUES (?, ?, ?, ?)"
        cursor.executemany(query, os_data_to_insert)
        db_connector.commit()
        
        print(f"-> Succès : {len(os_data_to_insert)} enregistrements chargés dans DimOs.")
        mappings['Os'] = os_mapping
        
    except Exception as e:
        print(f"ERREUR lors du chargement de DimOs: {e}")
        db_connector.rollback()
        mappings['Os'] = {}

    # DimCalendar
    print("Chargement de DimCalendar...")
    try:
        # Obtenir toutes les dates uniques des colonnes pertinentes
        date_submitted = pd.to_datetime(data['Date Submitted'], errors='coerce')
        date_updated = pd.to_datetime(data['Updated'], errors='coerce')
        
        all_dates = pd.unique(np.concatenate((
            date_submitted.dropna(),
            date_updated.dropna()
        )))
        
        unique_dates_df = pd.DataFrame(all_dates, columns=['Date']).drop_duplicates()
        unique_dates_df = unique_dates_df[unique_dates_df['Date'].notna()]
        unique_dates_df['Date'] = pd.to_datetime(unique_dates_df['Date'])
        
        # Créer les attributs du calendrier
        # L'ID de date YYYYMMDD est une pratique courante
        unique_dates_df['DateId'] = unique_dates_df['Date'].dt.strftime('%Y%m%d').astype(int)
        unique_dates_df['Day'] = unique_dates_df['Date'].dt.day
        unique_dates_df['Month'] = unique_dates_df['Date'].dt.month
        unique_dates_df['Year'] = unique_dates_df['Date'].dt.year
        
        calendar_data_to_insert = [
            (row.DateId, row.Date.date(), row.Day, row.Month, row.Year)
            for row in unique_dates_df.itertuples(index=False)
        ]
        
        # Ajouter un enregistrement 'Unknown' (ex: 1900-01-01 avec ID 0)
        unknown_date_id = 0
        unknown_date_record = (unknown_date_id, datetime(1900, 1, 1).date(), 1, 1, 1900)
        
        # Éviter les doublons si 1900-01-01 existe déjà
        if not any(rec[0] == unknown_date_id for rec in calendar_data_to_insert):
            calendar_data_to_insert.insert(0, unknown_date_record)
        
        cursor = db_connector.cursor()
        cursor.execute("DELETE FROM DimCalendar")
        
        query = "INSERT INTO DimCalendar (DateId, [Date], [Day], [Month], [Year]) VALUES (?, ?, ?, ?, ?)"
        cursor.executemany(query, calendar_data_to_insert)
        db_connector.commit()
        
        print(f"-> Succès : {len(calendar_data_to_insert)} enregistrements chargés dans DimCalendar.")
        # Le mapping pour les dates est la conversion YYYYMMDD, 
        # mais nous stockons l'ID 'Unknown' pour la table de faits.
        mappings['Calendar_Unknown_Id'] = unknown_date_id
        
    except Exception as e:
        print(f"ERREUR lors du chargement de DimCalendar: {e}")
        db_connector.rollback()

    print("\n--- Chargement des dimensions terminé. ---")
    return mappings

# =============================================================================
# FONCTION DE CHARGEMENT DE LA TABLE DE FAITS
# =============================================================================

def load_fact_table(data, db_connector, mappings):
    """
    Transforme le DataFrame source et charge la table de faits FactBug
    en utilisant les mappings de clés de substitution.
    """
    print("\n--- Démarrage du chargement de FactBug ---")
    
    # 1. Créer un nouveau DataFrame pour la table de faits
    fact_table = pd.DataFrame()

    # 2. Colonnes directes
    fact_table['BugId'] = data['Id']
    fact_table['Summary'] = data['Summary'].fillna('No Summary')

    # 3. Gestion des colonnes SCD (Slowly Changing Dimensions)
    # Pour un chargement snapshot, tout est "actuel"
    fact_table['IsCurrent'] = 1 
    fact_table['SDC_EndDate'] = None # NULL
    
    # 4. Mappage des dimensions simples
    # .map() est très efficace. Il utilisera le dictionnaire 'mappings'.
    # Si une valeur n'est pas trouvée, elle deviendra NaN.
    fact_table['ProjectId'] = data['Project'].map(mappings['Project'])
    fact_table['ReporterId'] = data['Reporter'].map(mappings['User'])
    fact_table['AssigneeId'] = data['Assigned To'].map(mappings['User'])
    fact_table['PriorityId'] = data['Priority'].map(mappings['Priority'])
    fact_table['SeverityId'] = data['Severity'].map(mappings['Severity'])
    fact_table['ReproducibilityId'] = data['Reproducibility'].map(mappings['Reproducibility'])
    fact_table['ProductVersionId'] = data['Product Version'].map(mappings['Version'])
    fact_table['VersionFixedId'] = data['Fixed in Version'].map(mappings['Version'])
    fact_table['CategoryId'] = data['Category'].map(mappings['Category'])
    fact_table['ViewStatusId'] = data['View Status'].map(mappings['Status'])
    fact_table['StatusId'] = data['Status'].map(mappings['Status'])
    fact_table['ResolutionId'] = data['Resolution'].map(mappings['Status'])
    
    # 5. Mappage de la dimension composite (DimOs)
    unknown_os_id = mappings['Os'].get(('Unknown', 'Unknown', 'Unknown'), 0)
    # Créer des tuples (Platform, OS, OS Version) pour chaque ligne
    os_tuples = list(zip(
        data['Platform'].astype(str), 
        data['OS'].astype(str), 
        data['OS Version'].astype(str)
    ))
    # Chercher chaque tuple dans le mapping, utiliser l'ID 'Unknown' par défaut
    fact_table['OsId'] = [mappings['Os'].get(t, unknown_os_id) for t in os_tuples]

    # 6. Mappage des dates (DimCalendar)
    unknown_date_id = mappings.get('Calendar_Unknown_Id', 0)
    
    # Convertir les dates en 'YYYYMMDD', remplir les erreurs/NaN avec l'ID inconnu
    date_submitted_ids = pd.to_datetime(data['Date Submitted'], errors='coerce') \
                           .dt.strftime('%Y%m%d') \
                           .fillna(unknown_date_id) \
                           .astype(int)
    
    date_updated_ids = pd.to_datetime(data['Updated'], errors='coerce') \
                         .dt.strftime('%Y%m%d') \
                         .fillna(unknown_date_id) \
                         .astype(int)

    fact_table['DateSubmittedId'] = date_submitted_ids
    fact_table['DateUpdatedId'] = date_updated_ids
    # SDC_StartDate est souvent la date de dernière mise à jour
    fact_table['SDC_StartDate'] = date_updated_ids 

    # 7. Remplacer les NaN restants (si un mapping a échoué) par 0 ('Unknown')
    fact_table = fact_table.fillna(0)

    # 8. Insérer les données dans FactBug
    try:
        cursor = db_connector.cursor()
        cursor.execute("DELETE FROM FactBug")
        print("Table FactBug vidée.")
        
        # S'assurer que les colonnes sont dans le bon ordre
        fact_columns_order = [
            'BugId', 'SDC_StartDate', 'SDC_EndDate', 'IsCurrent', 'Summary',
            'DateSubmittedId', 'DateUpdatedId', 'ProjectId', 'ReporterId',
            'AssigneeId', 'PriorityId', 'SeverityId', 'ReproducibilityId',
            'ProductVersionId', 'VersionFixedId', 'CategoryId', 'OsId',
            'ViewStatusId', 'StatusId', 'ResolutionId'
        ]
        
        # Convertir le DataFrame en liste de tuples pour executemany
        data_to_insert = list(fact_table[fact_columns_order].itertuples(index=False, name=None))
        
        query = """
        INSERT INTO FactBug (
            BugId, SDC_StartDate, SDC_EndDate, [IsCurrent], [Summary],
            DateSubmittedId, DateUpdatedId, ProjectId, ReporterId,
            AssigneeId, PriorityId, SeverityId, ReproducibilityId,
            ProductVersionId, VersionFixedId, CategoryId, OsId,
            ViewStatusId, StatusId, ResolutionId
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.executemany(query, data_to_insert)
        db_connector.commit()
        print(f"-> Succès : {len(data_to_insert)} enregistrements chargés dans FactBug.")
        
    except Exception as e:
        print(f"ERREUR lors du chargement de FactBug: {e}")
        db_connector.rollback()
        
    print("--- Chargement de la table de faits terminé. ---")