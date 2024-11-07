import csv
import schedule
import time
from pymongo import MongoClient
from datetime import datetime

# Connexion à la bdd MongoDB
client = MongoClient("mongodb://localhost:27017")
# Nom de notre bdd
db = client.sae5bdd

# Importer en CSV
def import_data_from_csv(filename: str, collection_name: str) -> None:
    """
    Importation des données à partir d'un fichier CSV dans la collection collection_name
    On n'importe pas un document s'il existe déjà, on vérifie avec son id
    
    Args:
        filename (str): Le nom du fichier CSV contenant les données à importer
        collection_name (str): Le nom de la collection

    Retourne:
        None

    Fonctionnement:
        - Lit le fichier CSV ligne par ligne
        - Crée un document adapté au schéma de la collection collection_name
        - Vérifie si le document existe déjà
        - Insère uniquement les documents qui n'existent pas dans la collection
    """
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # On récupère un champs unique qui permettra de vérifier si le document existe déjà
            if collection_name == "Users":
                unique_field = {"email": row['email']}
            elif collection_name == "Groups":
                unique_field = {"name": row['name']}
            elif collection_name == "Pages":
                unique_field = {"name": row['name']}
            elif collection_name in ["Posts", "Messages"]:
                unique_field = {"id": row['id']}
            else:
                print(f"La collection {collection_name} n'existe pas")
                return

            # Vérifie si le document existe déjà et on l'ignore si existe déjà
            if db[collection_name].find_one(unique_field):
                continue

            # Création du document en fonction de la collection
            if collection_name == "Users":
                document = {
                    "id": row['id'],
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
                    "id": row['id'],
                    "name": row['name'],
                    "description": row['description'],
                    "members": row['members'].split(',') if row['members'] else [],
                    "created_by": row['created_by'],
                    "created_at": row['created_at']
                }
            elif collection_name == "Posts":
                document = {
                    "id": row['id'],
                    "user_id": row['user_id'],
                    "content": row['content'],
                    "media_url": row['media_url'],
                    "likes": row['likes'].split(',') if row['likes'] else [],
                    "comments": eval(row['comments']) if row['comments'] else [],
                    "created_at": row['created_at']
                }
            elif collection_name == "Messages":
                document = {
                    "id": row['id'],
                    "sender_id": row['sender_id'],
                    "receiver_id": row['receiver_id'],
                    "content": row['content'],
                    "created_at": row['created_at']
                }
            elif collection_name == "Pages":
                document = {
                    "id": row['id'],
                    "name": row['name'],
                    "description": row['description'],
                    "followers": row['followers'].split(',') if row['followers'] else [],
                    "created_by": row['created_by'],
                    "created_at": row['created_at']
                }
            
            # Insérer le document dans la collection
            db[collection_name].insert_one(document)
    print("Importation csv: terminé")

# On importe des données d'exemple
# import_data_from_csv('csv data/imports/users_data.csv', 'Users')
# import_data_from_csv('csv data/imports/groups_data.csv', 'Groups')
# import_data_from_csv('csv data/imports/posts_data.csv', 'Posts')
# import_data_from_csv('csv data/imports/messages_data.csv', 'Messages')
# import_data_from_csv('csv data/imports/pages_data.csv', 'Pages')


# Exporter en CSV
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
        - Détecte tous les champs uniques parmi les documents pour les utiliser comme en-têtes du CSV
        - Écrit chaque document dans le fichier CSV
    """
    collection = db[collection_name]
    documents = list(collection.find())
    
    if not documents:
        print(f"La collection {collection_name} ne contient aucun document")
        return

    # On prépare les en-têtes du CSV en parcourant tout les documents
    # car certains documents peuvent avoir des champs vides
    all_fields = set()
    for doc in documents:
        all_fields.update(doc.keys())
    all_fields = sorted(all_fields)

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fields)
        # On écrit les en-têtes dans le csv, ceux indiqués dans le paramètre "fieldnames" du writer
        writer.writeheader()
        
        for doc in documents:
            # On créer la ligne csv à partir des données du document, on met une chaîne vide s'il y a rien
            row = {field: doc.get(field, '') for field in all_fields}
            print(row)
            writer.writerow(row)

    print("Exporation CSV: terminé")    

# Exemple d'utilisation pour exporter les collections
# export_data_to_csv('csv data/exports/users_export.csv', 'Users')
# export_data_to_csv('csv data/exports/groups_export.csv', 'Groups')
# export_data_to_csv('csv data/exports/posts_export.csv', 'Posts')
# export_data_to_csv('csv data/exports/messages_export.csv', 'Messages')
# export_data_to_csv('csv data/exports/pages_export.csv', 'Pages')

# Sauvegarde de la bdd MongoDB
def daily_backup():
    """
    Fonction de sauvegarde de la bdd : Exporte toutes les collections vers des fichiers CSV
    """
    # Liste des collections à sauvegarder
    collections = ["Users", "Groups", "Posts", "Messages", "Pages"]
    
    # Date actuelle pour nommer les fichiers
    current_date = datetime.now().strftime("%d-%m-%Y")
    
    for collection in collections:
        # Nom du fichier avec la date
        filename = f"{collection.lower()}_export_{current_date}.csv"
        export_data_to_csv(filename, collection)
    
    print("Sauvegarde terminée.")

# Planifier la sauvegarde tous les jours à 00h00
schedule.every().day.at("00:00").do(daily_backup)

# Boucle pour vérifier s'il est temps de faire la sauvegarde
while True:
    print("En attente de la prochaine sauvegarde...")
    schedule.run_pending()
    time.sleep(120)