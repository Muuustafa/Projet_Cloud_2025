import os
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound

# Configuration des chemins et noms
BUCKET_NAME = "m2-dsia-mamadou-moustapha-diallo-data"
INPUT_FOLDER = "input/"
CLEAN_FOLDER = "clean/"
ERROR_FOLDER = "error/"
DONE_FOLDER = "done/"
PROJECT_ID = "isi-group-m2-dsia"
DATASET_ID = "dataset_mamadou_mustafa_diallo"
TABLE_ID = "transactions"

# Initialisation des clients GCP
storage_client = storage.Client(project=PROJECT_ID)
bigquery_client = bigquery.Client(project=PROJECT_ID)
bucket_client = storage_client.get_bucket(BUCKET_NAME)

def validate_and_clean(df):
    
    # Valide et nettoie les données.
    
    required_columns = ["transaction_id", "product_name", "category", "price", "quantity", "date"]
    
    # Vérifier les colonnes requises
    if not all(column in df.columns for column in required_columns):
        return None, "Colonnes manquantes"
    
    # Supprimer les lignes avec des valeurs manquantes
    df = df.dropna(subset=required_columns)
    
    # Vérifier que le prix et la quantité sont positifs
    df = df[(df["price"] > 0) & (df["quantity"] > 0)]
    
    # Convertir la colonne "date" en format date
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"].notna()]
    
    return df, None

def process_file(file_name):

    # Traite un fichier : validation, nettoyage, et chargement dans BigQuery.

    try:
        # Télécharger le fichier depuis Cloud Storage
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{INPUT_FOLDER}{file_name}")
        local_path = f"files/{file_name}"
        blob.download_to_filename(local_path)
        
        # Charger le fichier en DataFrame
        df = pd.read_csv(local_path)
        
        # Valider et nettoyer les données
        cleaned_df, error = validate_and_clean(df)
        if error:
            print(f"Erreur dans le fichier {file_name} : {error}")
            # Déplacer le fichier vers error/
            blob.upload_from_filename(local_path, destination_blob_name=f"{ERROR_FOLDER}{file_name}")
        else:
            print(f"Fichier {file_name} nettoyé avec succès.")
            # Déplacer le fichier vers clean/
            blob.upload_from_filename(local_path, destination_blob_name=f"{CLEAN_FOLDER}{file_name}")
            
            # Charger les données dans BigQuery
            table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
            job = bigquery_client.load_table_from_dataframe(cleaned_df, table_ref)
            job.result()  # Attendre la fin du job
            
            print(f"Données du fichier {file_name} chargées dans BigQuery.")
            
            # Déplacer le fichier vers done/
            blob.upload_from_filename(local_path, destination_blob_name=f"{DONE_FOLDER}{file_name}")
        
        # Supprimer le fichier temporaire
        os.remove(local_path)
        
    except Exception as e:
        print(f"Erreur lors du traitement du fichier {file_name} : {str(e)}")

def main():
    
    # Fonction principale pour traiter tous les fichiers dans input/.

    # Lister les fichiers dans input/
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=INPUT_FOLDER)
    
    for blob in blobs:
        if blob.name.endswith(".csv"):
            file_name = blob.name.split("/")[-1]
            print(f"Traitement du fichier : {file_name}")
            process_file(file_name)

if __name__ == "__main__":
    main()