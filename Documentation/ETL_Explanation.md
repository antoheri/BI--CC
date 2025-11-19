# Documentation technique du script `etl_pipeline.py`

## 1. Introduction

Ce document décrit en détail le fonctionnement du script Python `etl_pipeline.py` que nous avons développé pour le projet d'analyse du bugtracker Scribus. Il explique :
- la structure générale du pipeline ETL,
- le rôle de chaque bloc de code (fonctions principales),
- la manière dont ce script met en œuvre les concepts vus dans les documents de cours (SSIS, SCD, DWH, OLAP).

L'objectif est de montrer clairement comment notre implémentation Python reproduit et automatise les étapes d'un ETL classique tel que présenté dans les supports de cours.



## 2. Vue d'ensemble du pipeline ETL

Le script `etl_pipeline.py` implémente un pipeline complet de type ETL (Extract – Transform – Load) organisé comme suit :

1. **Extraction** : téléchargement automatique des fichiers CSV Scribus depuis un serveur web et chargement en mémoire (Pandas).
2. **Transformation** : nettoyage, normalisation, conversion de types et préparation des données pour le chargement dans le DWH.
3. **Chargement** :
   - mise à jour des dimensions (SCD Type 1) via des commandes SQL MERGE,
   - insertion des faits historisés (SCD Type 2) dans `FactBug`.

Enfin, une boucle principale orchestre ces étapes pour tous les snapshots disponibles.



## 3. Structure du script et explication bloc par bloc

### 3.1 Imports et dépendances

Le script commence par l'import des bibliothèques nécessaires :

- `pandas`, `numpy` : manipulation de données tabulaires (DataFrame).
- `requests`, `BeautifulSoup` : récupération et parsing des pages HTML pour trouver les fichiers CSV à télécharger.
- `tqdm` : affichage de barres de progression lors des téléchargements.
- `datetime`, `timedelta`, `date` : gestion des dates (snapshots, SDC_StartDate, etc.).
- `re` : expressions régulières (extraction de dates depuis le nom des fichiers).
- `os`, `glob` : gestion des fichiers et dossiers locaux.
- `dotenv`, `getenv` : chargement des variables de configuration (chaîne de connexion SQL).
- `mssql_python.connect` : wrapper permettant de se connecter à SQL Server.

Ces imports fournissent l'infrastructure technique nécessaire à un ETL automatisé.

### 3.2 Extraction : téléchargement et lecture des fichiers CSV

#### 3.2.1 `get_csv_from_url(URL, file_path='./data/')`

Cette fonction :

1. Télécharge le HTML de la page `URL`.
2. Analyse le contenu avec BeautifulSoup pour trouver les liens vers les fichiers CSV du bugtracker.
3. Filtre les liens pour ne garder que les fichiers correspondant aux dumps Scribus.
4. Compare la liste des fichiers trouvés avec les fichiers déjà présents dans le dossier local `file_path`.
5. Télécharge uniquement les fichiers manquants, avec une barre de progression par fichier.

Elle joue le rôle d'une étape d'extraction automatisée, remplaçant le couple "source de fichiers plats + Foreach Loop" vu dans SSIS.

#### 3.2.2 `get_data_from_file(file_path)`

Cette fonction :

1. Charge un fichier CSV dans un DataFrame Pandas.
2. Utilise une expression régulière pour extraire la date du snapshot à partir du nom du fichier (format `YYYY-MM-DD`).
3. Si aucune date n'est trouvée, utilise la date du jour.
4. Renvoie le DataFrame et la date de chargement (snapshot).

La date extraite est utilisée ensuite comme `SDC_StartDate` dans la logique SCD2 de la table de faits.

### 3.3 Transformation : nettoyage et préparation

#### 3.3.1 `clean_data(data)`

Cette fonction applique les transformations suivantes :

- Remplacement des valeurs manquantes dans certaines colonnes textuelles par une valeur standardisée (`"Unknown"`).
- Conversion en minuscules de colonnes de type catégoriel (priority, severity, reproducibility, etc.) afin d'éviter les doublons logiques dus à la casse.
- Conversion des colonnes de dates (`Date Submitted`, `Updated`) en type datetime.
- Suppression des doublons éventuels dans les données.

Elle correspond aux étapes de nettoyage et de standardisation présentées dans les cours (Derived Columns, Data Cleaning).

#### 3.3.2 `prepare_data_for_staging(data, loaded_date)`

Cette fonction :

- Renomme les colonnes du DataFrame pour qu'elles correspondent à la nomenclature du DWH (par exemple `Id` → `BugId`, `Project` → `ProjectName`, `Reporter` → `ReporterName`, etc.).
- Ajoute une colonne `SDC_StartDate` définie à la date de snapshot extraite précédemment.

L'idée est de préparer les données à être utilisées pour la mise à jour des dimensions et le chargement de la table de faits, dans un format cohérent avec le schéma du Data Warehouse.

### 3.4 Chargement : dimensions et faits

#### 3.4.1 Connexion à la base : `connect_to_db()`

Cette fonction :

1. Charge les variables d'environnement à partir du fichier `.env`.
2. Récupère la chaîne de connexion SQL (`SQL_CONNECTION_STRING`).
3. Ouvre une connexion à SQL Server via `mssql_python.connect`.
4. Renvoie l'objet de connexion pour être utilisé dans les fonctions de chargement.

#### 3.4.2 Mise à jour des dimensions : `_merge_simple_dimension(...)`

Cette fonction utilitaire est utilisée pour toutes les dimensions simples (Project, Priority, Severity, Reproducibility, Version, Category, Status, etc.). Son fonctionnement :

1. Construire une liste de valeurs uniques (texte) à partir d'une série Pandas, en éliminant les valeurs nulles ou `"Unknown"`.
2. Créer une table temporaire SQL (`#StageXXX`) avec une seule colonne NVARCHAR.
3. Insérer toutes les valeurs uniques dans cette table temporaire.
4. Exécuter une commande `MERGE` entre la table temporaire et la dimension cible, en insérant uniquement les valeurs absentes de la dimension.
5. Supprimer la table temporaire.

On obtient ainsi un comportement équivalent au remplissage d'une dimension de type SCD1 dans SSIS : pas d'historisation, ajout des nouvelles valeurs uniquement.

#### 3.4.3 `update_dimensions_staging(data, db_connector)`

Cette fonction :

- Crée un curseur SQL sur la connexion.
- Appelle `_merge_simple_dimension` pour alimenter les dimensions :
  - DimProject (ProjectName)
  - DimUser (ReporterName et AssigneeName)
  - DimPriority
  - DimSeverity
  - DimReproducibility
  - DimVersion (ProductVersionName et VersionFixedName)
  - DimCategory
  - DimStatus (ViewStatusName, StatusName, ResolutionName)
- Traite séparément la dimension DimOs, qui dépend de plusieurs colonnes (Platform, OS, OSVersion), en utilisant également une table temporaire et un MERGE.
- Met à jour DimCalendar en insérant toutes les dates utiles (dates de soumission, de mise à jour, de début et de fin SDC). Les dates sont transformées en clé DateId au format `YYYYMMDD` et insérées via une table temporaire et un MERGE.

Cette étape assure que toutes les dimensions nécessaires à FactBug sont complètes et à jour avant le chargement des faits.

#### 3.4.4 Chargement de la table de faits SCD2 : `load_fact_snapshot_scd2_staging(data, db_connector)`

Cette fonction réalise le cœur de la logique SCD2 de notre ETL :

1. Préparation des données :
   - Ajout de colonnes de dates formatées pour SQL (DateSubmitted_SQL, DateUpdated_SQL, SDC_StartDate_SQL).
   - Remplacement des valeurs manquantes par `None` (NULL SQL).
2. Création d'une table temporaire `#StageFact` dans SQL, avec les colonnes nécessaires (BugId, noms textuels, dates, etc.).
3. Insertion de toutes les lignes du DataFrame dans `#StageFact`.
4. Fermeture des anciennes versions SCD2 :
   - Calcul de `SDC_EndDate` comme la veille de `SDC_StartDate`.
   - Mise à jour de `FactBug` pour toutes les lignes ayant le même BugId et `IsCurrent = 1`, en mettant `IsCurrent = 0` et en renseignant `SDC_EndDate`.
5. Insertion des nouvelles lignes :
   - `INSERT INTO FactBug` en sélectionnant depuis `#StageFact` et en joignant toutes les dimensions (DimProject, DimUser, DimPriority, DimSeverity, DimReproducibility, DimVersion, DimCategory, DimStatus, DimOs, DimCalendar).
   - Les jointures fournissent les clés substituts nécessaires à la fact table.
   - Les nouvelles lignes ont `IsCurrent = 1`, `SDC_StartDate` renseigné et `SDC_EndDate` à NULL.
6. Suppression de la table temporaire et commit de la transaction.

Le résultat est une table de faits historisée qui conserve toutes les versions successives du même bug, en cohérence avec le concept de SCD Type 2.

### 3.5 Orchestration : fonction `main()`

La fonction `main()` orchestre l'ensemble du pipeline :

1. Définition de l'URL source et du dossier de stockage local.
2. Appel à `get_csv_from_url` pour synchroniser les fichiers CSV.
3. Liste des fichiers CSV disponibles et triés.
4. Connexion à la base de données via `connect_to_db()`.
5. Pour chaque fichier CSV :
   - chargement des données (`get_data_from_file`),
   - nettoyage (`clean_data`),
   - préparation pour le staging (`prepare_data_for_staging`),
   - mise à jour des dimensions (`update_dimensions_staging`),
   - chargement de la fact table avec SCD2 (`load_fact_snapshot_scd2_staging`).
6. Gestion des exceptions éventuelles et fermeture de la connexion.

Cette fonction joue le rôle de "Control Flow" que l'on retrouve dans les packages SSIS.

---

## 4. Comparaison avec les documents de cours

Dans cette section, nous montrons comment notre script se compare aux différents documents de cours fournis (SSIS, SCD, création de DWH, introduction à l'OLAP).

### 4.1 Comparaison avec 1_SSIS (ETL avec SSIS)

Le document SSIS décrit un processus ETL structuré en :

- Control Flow : enchaînement des tâches (import, nettoyage, chargement),
- Data Flow : lecture de fichiers, transformations, lookups, chargement SQL.

Notre script reproduit cette logique de la manière suivante :

- Le rôle du **Control Flow** est assuré par la fonction `main()` et la boucle sur les fichiers.
- Le rôle du **Data Flow** est assuré par la combinaison :
  - `get_data_from_file` (source),
  - `clean_data` et `prepare_data_for_staging` (transformations),
  - `update_dimensions_staging` et `load_fact_snapshot_scd2_staging` (destination).

Les composants SSIS tels que :
- Flat File Source,
- Derived Column,
- Data Conversion,
- Lookup,
- OLE DB Destination,

sont remplacés par :
- Pandas pour la lecture et la transformation,
- des commandes SQL MERGE,
- des jointures explicites dans l'INSERT de FactBug.

### 4.2 Comparaison avec 2_SCD (Slowly Changing Dimensions)

Le document sur les SCD introduit :

- SCD Type 1 (mise à jour sans historique),
- SCD Type 2 (nouvelle ligne avec dates de validité),
- la notion de StartDate et EndDate.

Notre script implémente exactement ces concepts :

- Les dimensions sont gérées en **SCD1** via des MERGE qui insèrent uniquement les nouvelles valeurs sans historisation.
- La table FactBug est gérée en **SCD2** :
  - fermeture des anciennes versions (IsCurrent=0, SDC_EndDate défini),
  - création d'une nouvelle version (IsCurrent=1, nouvelle SDC_StartDate).

Ainsi, la logique SCD enseignée pour SSIS est intégralement reproduite en SQL piloté par Python.

### 4.3 Comparaison avec 6a_Create_Database (création de DWH)

Le document sur la création du DWH présente :

- la création de la base,
- la définition des dimensions et de la table de faits,
- la création d'une dimension calendrier,
- l'utilisation d'un schéma en étoile.

Notre script :

- se base sur ce schéma (DimProject, DimUser, DimPriority, DimSeverity, DimReproducibility, DimVersion, DimCategory, DimStatus, DimOs, DimCalendar, FactBug),
- remplit DimCalendar avec les dates rencontrées dans les snapshots, en respectant le format de clé DateId,
- assure le lien correct entre FactBug et toutes les dimensions via les jointures au moment de l'INSERT.

La structure logique et les bonnes pratiques vues dans ce document sont donc bien respectées.

### 4.4 Comparaison avec la théorie BI / OLAP (BI-CC et cube)

Les documents théoriques sur l'OLAP et les cubes expliquent :

- la séparation entre DWH (stockage intégré) et cube (modèle d'analyse),
- l'importance d'un schéma en étoile bien conçu,
- les opérations OLAP (drill-down, roll-up, slice, dice).

En fournissant un DWH propre, historisé et cohérent, notre script ETL :

- prépare directement les données pour la création d'un modèle Tabular ou d'un cube OLAP,
- permet l'analyse par temps, version, OS, sévérité, module, développeur, etc.,
- rend possible l'utilisation de mesures DAX et de hiérarchies telles que décrites dans la partie Analysis du projet.

En ce sens, notre pipeline est aligné avec l'architecture BI présentée dans les cours.

---

## 5. Conclusion

Le script `etl_pipeline.py` constitue une implémentation complète d'un processus ETL moderne, en s'appuyant sur :

- des outils de traitement de données (Pandas),
- des téléchargements automatisés,
- des commandes SQL robustes (MERGE, INSERT avec jointures),
- une gestion correcte des SCD Type 1 et Type 2,
- une orchestration claire via la fonction `main()`.

Il reproduit fidèlement les étapes et les concepts vus dans les documents de cours (SSIS, SCD, création de DWH, OLAP), tout en offrant la flexibilité d'une implémentation en Python. Ce pipeline forme ainsi la base fiable et automatisée de notre Data Warehouse DWBugs et alimente le modèle analytique utilisé dans la suite du projet.
