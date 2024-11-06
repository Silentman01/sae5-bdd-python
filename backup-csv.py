import csv
import schedule
import time
from pymongo import MongoClient
from datetime import datetime

# Connexion à la base de données MongoDB
client = MongoClient("mongodb://localhost:27017")
# Nom de notre bdd
db = client.sae5bdd

def import_data_from_csv(filename: str, collection_name: str) -> None:
    """
    Importation des données à partir d'un fichier CSV dans la collection collection_name
    
    Args:
        filename (str): Le nom du fichier CSV contenant les données à importer
        collection_name (str): Le nom de la collection

    Retourne:
        None

    Fonctionnement:
        - Lit le fichier CSV ligne par ligne
        - Crée un document adapté au schéma de la collection collection_name
        - Insère chaque document dans la collection collection_name
    """
    # Lecture du fichier CSV
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Création du document en fonction de la collection
            if collection_name == "Users":
                document = {
                    "username": row['username'],
                    "avatar_url": row['avatar_url'],
                    "full_name": row['full_name'],
                    "email": row['email'],
                    "password": row['password'],
                    "birthdate": row['birthdate'],
                    "interests": row['interests'].split(',') if row['interests'] else [],
                    "friends": row['friends'].split(',') if row['friends'] else [],
                    "groups": row['groups'].split(',') if row['groups'] else [],
                    "pages": row['pages'].split(',') if row['pages'] else [],
                    "created_at": row['created_at']
                }
            elif collection_name == "Groups":
                document = {
                    "name": row['name'],
                    "description": row['description'],
                    "members": row['members'].split(',') if row['members'] else [],
                    "created_by": row['created_by'],
                    "created_at": row['created_at']
                }
            elif collection_name == "Posts":
                document = {
                    "user_id": row['user_id'],
                    "content": row['content'],
                    "media_url": row['media_url'],
                    "likes": row['likes'].split(',') if row['likes'] else [],
                    "comments": eval(row['comments']) if row['comments'] else [],
                    "created_at": row['created_at']
                }
            elif collection_name == "Messages":
                document = {
                    "sender_id": row['sender_id'],
                    "receiver_id": row['receiver_id'],
                    "content": row['content'],
                    "created_at": row['created_at']
                }
            elif collection_name == "Pages":
                document = {
                    "name": row['name'],
                    "description": row['description'],
                    "followers": row['followers'].split(',') if row['followers'] else [],
                    "created_by": row['created_by'],
                    "created_at": row['created_at']
                }
            else:
                print(f"La collection {collection_name} n'existe pas.")
                return

            # Insérer le document dans la collection
            db[collection_name].insert_one(document)
    
    print(f"Import des données dans la collection {collection_name} terminé.")

# Exemple d'utilisation pour importer dans chaque collection
# import_data_from_csv('csv data/imports/users_data.csv', 'Users')
# import_data_from_csv('csv data/imports/groups_data.csv', 'Groups')
# import_data_from_csv('csv data/imports/posts_data.csv', 'Posts')
# import_data_from_csv('csv data/imports/messages_data.csv', 'Messages')
# import_data_from_csv('csv data/imports/pages_data.csv', 'Pages')


def export_data_to_csv(filename: str, collection_name: str) -> None:
    """
    Exportation des données à partir de la collection collection_name dans un fichier CSV
    
    Args:
        filename (str): Le nom du fichier CSV contenant les données à importer
        collection_name (str): Le nom de la collection

    Retourne:
        None

    Fonctionnement:
        - Récupère tous les documents de la collection collection_name
        - Détecte tous les champs uniques parmi les documents pour les utiliser comme en-têtes CSV
        - Écrit chaque document dans le fichier CSV, en assurant que tous les champs sont présents dans chaque ligne
    """
    # Accéder à la collection
    collection = db[collection_name]
    
    # Récupérer tous les documents de la collection
    documents = list(collection.find())
    
    # Vérifier si la collection est vide
    if not documents:
        print(f"La collection {collection_name} est vide. Aucun fichier exporté.")
        return

    # Détecter tous les champs uniques dans les documents
    all_fields = set()
    for doc in documents:
        all_fields.update(doc.keys())
    all_fields = sorted(all_fields)  # On tri les champs

    # Écriture des données dans le fichier CSV
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fields)
        writer.writeheader()
        
        # Parcourir chaque document et les mettre dans le CSV
        for doc in documents:
            # Créer une ligne pour chaque document avec des champs manquants remplis par des chaînes vides
            row = {field: doc.get(field, '') for field in all_fields}
            # Convertir l'ObjectId en string
            row['_id'] = str(row['_id'])
            writer.writerow(row)
    
    print(f"Export des données de la collection {collection_name} vers {filename} terminé.")

# Exemple d'utilisation pour exporter les collections
#export_data_to_csv('csv data/exports/users_export.csv', 'Users')
# export_data_to_csv('csv data/exports/groups_export.csv', 'Groups')
# export_data_to_csv('csv data/exports/posts_export.csv', 'Posts')
# export_data_to_csv('csv data/exports/messages_export.csv', 'Messages')
# export_data_to_csv('csv data/exports/pages_export.csv', 'Pages')


def daily_backup():
    """
    Fonction de sauvegarde de la bdd : Exporte toutes les collections vers des fichiers CSV individuels
    avec un nom de fichier unique incluant la date de sauvegarde.
    """
    # Liste des collections à sauvegarder
    collections = ["Users", "Groups", "Posts", "Messages", "Pages"]
    
    # Date actuelle pour nommer les fichiers
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for collection in collections:
        # Nom du fichier avec la date pour éviter l'écrasement
        filename = f"{collection.lower()}_backup_{current_date}.csv"
        export_data_to_csv(filename, collection)
    
    print("Sauvegarde terminée.")

# Planifier la sauvegarde quotidienne à 00h00
schedule.every().day.at("00:00").do(daily_backup)

# Boucle pour vérifier s'il est temps de faire la sauvegarde
while True:
    print("En attente de la prochaine sauvegarde...")
    schedule.run_pending()
    time.sleep(120)