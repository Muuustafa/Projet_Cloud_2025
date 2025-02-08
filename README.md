# Projet GCP : Traitement et Chargement de Données

Ce projet consiste à traiter des données transactionnelles en utilisant Google Cloud Platform (GCP). Les données sont stockées dans un bucket Cloud Storage, nettoyées et validées via un script Python, puis chargées dans une table BigQuery.

---

## Objectifs du Projet

1. **Configurer les ressources GCP** :
   - Créer un bucket Cloud Storage avec des dossiers pour organiser les fichiers.
   - Créer une table BigQuery pour stocker les données nettoyées.

2. **Créer un script Python** :
   - Récupérer les fichiers depuis Cloud Storage.
   - Valider et nettoyer les données.
   - Charger les données nettoyées dans BigQuery.
   - Déplacer les fichiers traités dans les dossiers appropriés.

3. **Automatiser le processus** :
   - Le script Python peut être exécuté manuellement ou planifié pour traiter les fichiers automatiquement.

---

## Configuration des Ressources GCP

### 1. Créer un Bucket Cloud Storage

1. Allez dans la console GCP.
2. Accédez à **Cloud Storage** > **Créer un bucket**.
3. Nommez le bucket : `m2dsia-[nom]-[prenom]-data` (remplacez `[nom]` et `[prenom]` par vos informations).
4. Créez les dossiers suivants dans le bucket :
   - `input/` : Contient les fichiers bruts.
   - `clean/` : Contiendra les données nettoyées.
   - `error/` : Contiendra les fichiers avec des erreurs.
   - `done/` : Contiendra les fichiers déjà chargés dans BigQuery.

### 2. Créer une Table BigQuery

1. Allez dans **BigQuery**.
2. Créez un dataset nommé `dataset_[nom_prenom]` (remplacez `[nom_prenom]` par vos informations).
3. Créez une table `transactions` dans ce dataset avec le schéma suivant :
   - `transaction_id` (INT64, Description: Identifiant unique)
   - `product_name` (STRING, NOT NULL)
   - `category` (STRING, NOT NULL)
   - `price` (FLOAT64, NOT NULL)
   - `quantity` (INT64, NOT NULL)
   - `date` (DATE, NOT NULL)
   - `customer_name` (STRING)
   - `customer_email` (STRING)
4. Configurez la table pour qu'elle soit **partitionnée par `date`** et **clusterisée par `category` et `product_name`**.

---

## Script Python

Le script Python réalise les étapes suivantes :
1. Récupère les fichiers du dossier `input/`.
2. Valide et nettoie les données.
3. Déplace les fichiers vers `clean/` ou `error/`.
4. Charge les données nettoyées dans BigQuery.
5. Déplace les fichiers traités vers `done/`.

### Fonctionnalités du Script

- **Validation des données** :
  - Vérifie que les colonnes requises sont présentes.
  - Supprime les lignes avec des valeurs manquantes ou invalides.
  - Vérifie que les valeurs de `price` et `quantity` sont positives.

- **Nettoyage des données** :
  - Convertit la colonne `date` en format date.
  - Supprime les lignes avec des dates invalides.

- **Chargement dans BigQuery** :
  - Les données nettoyées sont chargées dans la table BigQuery `transactions`.

- **Gestion des fichiers** :
  - Les fichiers sont déplacés vers les dossiers `clean/`, `error/`, ou `done/` en fonction de leur état.

---

## Instructions pour Exécuter le Script

### 1. Prérequis

- Python 3.7 ou supérieur.
- Compte Google Cloud Platform (GCP) avec un projet activé.
- Fichier de credentials GCP au format JSON.

### 2. Installation des Dépendances

Installez les packages Python nécessaires :

```bash
pip install pandas google-cloud-storage google-cloud-bigquery
