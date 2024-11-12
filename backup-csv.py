from pymongo import MongoClient
import csv
from bson import ObjectId
import schedule
import time
from datetime import datetime
from bson.binary import Binary

# Connexion à la bdd MongoDB
client = MongoClient("mongodb://localhost:27017")
# bdd de test
#db = client.sae5bdd
db = client.socialnetworkdb

# Fonction pour converti de binaire en un string car on peut pas enregistrer en Binary directement
def binary_id_to_str(id):
    """
    Convertit un champ de type Binary en chaîne de caractères.
    """
    if isinstance(id, Binary):
        return id.hex()
    return str(id)

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
    with open(filename, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # On récupère un champs unique qui permettra de vérifier si le document existe déjà
            if collection_name == "users":
                unique_field = {"mail": row['mail']}
            elif collection_name == "group":
                unique_field = {"name": row['name']}
            elif collection_name == "pages":
                unique_field = {"name": row['name']}
            elif collection_name in ["posts", "privates_messages"]:
                # Pas besoin de vérifier pour posts et privates_messages car il peut en avoir plusieurs identiques
                unique_field = None
            else:
                print(f"La collection {collection_name} n'existe pas")
                return

            # Vérifie si le document existe déjà et on l'ignore si existe déjà
            if db[collection_name].find_one(unique_field):
                continue

            # Création du document en fonction de la collection
            if collection_name == "users":
                document = {
                    "username": row['username'],
                    "avatar": row['avatar'],
                    "bio": row['bio'],
                    "interests": row['interests'].split(',') if row['interests'] else [],
                    "first_name": row['first_name'],
                    "last_name": row['last_name'],
                    "mail": row['mail'],
                    "password": row['password'],
                    "role": row['role'],
                    "birthdate": row['birthdate'],
                    "friends": row['friends'].split(',') if row['friends'] else [],
                    "groups": row['groups'].split(',') if row['groups'] else [],
                    "pages": row['pages'].split(',') if row['pages'] else [],
                    "createdAt": row['createdAt']
                }
            elif collection_name == "group":
                document = {
                    "name": row['name'],
                    "description": row['description'],
                    "members": row['members'].split(',') if row['members'] else [],
                    "createdBy": row['createdBy'],
                    "createdAt": row['createdAt']
                }
            elif collection_name == "posts":
                document = {
                    "userId": row['userId'],
                    "content": row['content'],
                    "image": row['image'],
                    "likes": row['likes'].split(',') if row['likes'] else [],
                    "comments": eval(row['comments']) if row['comments'] else [],
                    "createdAt": row['createdAt']
                }
            elif collection_name == "privates_messages":
                document = {
                    "sender_id": row['sender_id'],
                    "receiver_id": row['receiver_id'],
                    "content": row['content'],
                    "createdAt": row['createdAt']
                }
            elif collection_name == "pages":
                document = {
                    "name": row['name'],
                    "description": row['description'],
                    "followers": row['followers'].split(',') if row['followers'] else [],
                    "createdBy": row['createdBy'],
                    "createdAt": row['createdAt']
                }
            
            # Insérer le document dans la collection
            db[collection_name].insert_one(document)
    print("Importation csv: terminé")

# On importe des données d'exemple
"""
import_data_from_csv('csv data/imports/user_data.csv', 'users')
import_data_from_csv('csv data/imports/group_data.csv', 'group')
import_data_from_csv('csv data/imports/post_data.csv', 'posts')
import_data_from_csv('csv data/imports/privates_messages_data.csv', 'privates_messages')
import_data_from_csv('csv data/imports/pages_data.csv', 'pages')
"""


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

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fields)
        # On écrit les en-têtes dans le csv, ceux indiqués dans le paramètre "fieldnames" du writer
        writer.writeheader()
        
        for doc in documents:
            row = {}
                
            # On insère tout les champs à partir du document, sauf _id car déjà inséré au dessus
            for field in all_fields:
                # Si un champs est de type Binary, on le converti en str car sinon il sera illisible dans le csv
                if type(doc.get(field)) == Binary:
                    valeur = binary_id_to_str(doc.get(field))
                
                # Si un champs est une liste non vide, et qu'elle contient des éléments de type Binary
                # on les convertis en str également
                elif type(doc.get(field)) == list and len(doc.get(field)) > 0 and type(doc.get(field)[0]) == Binary:
                    valeur = [binary_id_to_str(element) for element in doc.get(field)]
                    print(valeur)
                else:
                    valeur = doc.get(field, '')

                row[field] = valeur

            writer.writerow(row)

    print("Exporation CSV: terminé")

# Exemple d'utilisation pour exporter les collections
"""export_data_to_csv('csv data/exports/pages_export.csv', 'pages')
export_data_to_csv('csv data/exports/user_export.csv', 'users')
export_data_to_csv('csv data/exports/group_export.csv', 'group')
export_data_to_csv('csv data/exports/post_export.csv', 'posts')
export_data_to_csv('csv data/exports/privates_messages_export.csv', 'privates_messages')
export_data_to_csv('csv data/exports/pages_export.csv', 'pages')"""


# Sauvegarde de la bdd MongoDB
def daily_backup():
    """
    Fonction de sauvegarde de la bdd : Exporte toutes les collections vers des fichiers CSV
    """
    # Liste des collections à sauvegarder
    collections = ["users", "group", "posts", "privates_messages", "pages"]
    
    # Date actuelle pour nommer les fichiers
    current_date = datetime.now().strftime("%d-%m-%Y")
    
    for collection in collections:
        # Nom du fichier avec la date
        filename = f"{collection.lower()}_export_{current_date}.csv"
        export_data_to_csv(filename, collection)
    
    print("Sauvegarde terminée.")

"""# Planifier la sauvegarde tous les jours à 00h00
schedule.every().day.at("00:00").do(daily_backup)

# Boucle pour vérifier s'il est temps de faire la sauvegarde
while True:
    print("En attente de la prochaine sauvegarde...")
    schedule.run_pending()
    time.sleep(120)"""