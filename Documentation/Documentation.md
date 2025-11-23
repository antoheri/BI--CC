# Documentation du projet
## Construction d’un Data Warehouse et d’un modèle OLAP pour l’analyse du bugtracker Scribus

## 1. Introduction

Ce projet a été réalisé dans le cadre du module 62-62 Data Exploitation. Son objectif était de construire un système décisionnel complet permettant d’analyser les données issues du bugtracker Scribus. Nous avons dû mettre en œuvre les différentes étapes d’un processus décisionnel moderne :

1. extraction et traitement automatique des snapshots CSV publiés hebdomadairement,
2. conception et alimentation d’un Data Warehouse basé sur un modèle en étoile,
3. gestion de l’historisation (SCD Type 2) dans la table de faits,
4. création d’un modèle OLAP Tabular avec SSAS,
5. développement de mesures analytiques en DAX,
6. préparation d’un modèle exploitable dans des outils tels que Power BI ou Excel.

---

## 2. Objectifs du projet

Les objectifs principaux étaient les suivants :

- comprendre et structurer les données du bugtracker Scribus,
- créer un schéma en étoile adapté aux besoins analytiques,
- développer un pipeline ETL automatisé en Python,
- gérer les dimensions en SCD Type 1 et la table de faits en SCD Type 2,
- construire un modèle Tabular dans SSAS basé sur le DWH produit,
- permettre l’analyse détaillée et multidimensionnelle de l’évolution des bugs.

Le système développé devait permettre de répondre à un ensemble de questions analytiques, notamment :

- évolution du nombre de bugs dans le temps,
- performance des développeurs,
- qualité des différentes versions du logiciel,
- répartition des bugs selon les modules,
- analyse de la reproductibilité et des environnements d’exécution.

---

## 3. Architecture globale du système

Le système complet repose sur une architecture classique de Business Intelligence composée de trois couches : Extraction, Transformation et Chargement.

### 3.1 Extraction

Les snapshots Scribus sont publiés sous forme de fichiers CSV accessibles depuis un répertoire web. Notre pipeline automatise leur récupération :

- il se connecte à la page listant les CSV,
- identifie les nouveaux fichiers en comparant avec le dossier local,
- télécharge uniquement les fichiers manquants,
- les sauvegarde dans un répertoire dédié.

Cette étape remplace l’usage d’un Foreach Loop Container tel qu’enseigné dans SSIS.

### 3.2 Transformation

Les fichiers CSV contiennent des valeurs manquantes, des données textuelles hétérogènes et des colonnes nécessitant une conversion de type. Les transformations suivantes ont été appliquées :

- normalisation des valeurs textuelles (minuscules, nettoyage),
- gestion des valeurs manquantes par des valeurs standardisées,
- conversion des dates en types appropriés,
- suppression des doublons,
- renommage des colonnes pour correspondre au schéma du DWH,
- ajout d’une colonne SDC_StartDate représentant la date du snapshot chargé.

### 3.3 Chargement

Le chargement s’effectue en deux étapes distinctes : dimensions et faits.

#### Dimensions (SCD Type 1)

Les dimensions sont chargées via un mécanisme utilisant des tables temporaires SQL et des commandes MERGE.
Cette approche permet :

- d’insérer uniquement les nouvelles valeurs distinctes,
- de conserver une seule version par membre,
- d’éviter toute historisation dans les dimensions.

Il s’agit d’un fonctionnement équivalent au composant Slowly Changing Dimension en mode SCD Type 1 dans SSIS.

#### Table de faits (SCD Type 2)

La table FactBug conserve l’historique des états d’un bug dans le temps.
Pour chaque snapshot chargé :

1. les lignes existantes correspondant aux mêmes BugId et marquées comme IsCurrent sont mises à jour :
   - IsCurrent passe à 0,
   - SDC_EndDate est fixé à la veille du chargement,
2. une nouvelle version du bug est insérée :
   - IsCurrent = 1,
   - SDC_StartDate correspond à la date du snapshot,
   - les clés substituts des dimensions sont résolues via jointures.

---

## 4. Modélisation du Data Warehouse

Nous avons conçu un schéma en étoile centré sur la table FactBug.

### 4.1 Table de faits : FactBug

FactBug contient les faits historisés et toutes les clés vers les dimensions.

### 4.2 Dimensions

Les dimensions suivantes ont été créées et alimentées :

- DimProject
- DimUser
- DimPriority
- DimSeverity
- DimReproducibility
- DimVersion
- DimCategory
- DimStatus
- DimOs
- DimCalendar

---

## 5. ETL en Python

Les fonctions principales sont :

- get_csv_from_url
- get_data_from_file
- clean_data
- prepare_data_for_staging
- update_dimensions_staging
- load_fact_snapshot_scd2_staging

---

## 6. Création du modèle analytique (OLAP)

### 6.1 Choix du modèle Tabular

Nous avons choisi Tabular pour sa simplicité et sa compatibilité avec Power BI.

### 6.2 Construction du modèle dans Visual Studio

Importation des tables, création des relations, gestion des relations inactives.

### 6.3 Création des mesures DAX

Mesures créées : Number of bugs, Fixed bugs, Open bugs, Reopened bugs, Fixed bugs per version, Fixed bugs per assignee.

### 6.4 Hiérarchies et perspectives

- calendrier : Year > Month > Day
- système d’exploitation : Platform > OS > OSVersion

### 6.5 Déploiement

Déploiement du modèle vers SSAS, utilisation via SSMS, Excel ou Power BI.

---

## 7. Analyse et exploitation

Le système permet de répondre aux questions analytiques du projet.

---

## 8. Conclusion

Le projet met en œuvre toute la chaîne décisionnelle : ETL, SCD, DWH, OLAP Tabular et mesures analytiques.
