import schedule
import time
from pymongo import MongoClient
from py2neo import Graph, Node, Relationship, NodeMatcher
from datetime import datetime

# Connexion à la base de données MongoDB
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client.sae5bdd

# Connexion à la base de données Neo4j
neo4j_graph = Graph("bolt://localhost:7687", auth=("neo4j", "rootroot"))
matcher = NodeMatcher(neo4j_graph)

def sync_users():
    """
    Synchronise la collection Users de MongoDB vers Neo4j sans supprimer les anciens nœuds.
    """
    for user in db.Users.find():
        user_id = str(user["_id"])  # Convertir ObjectId en string sinon j'ai une erreur
        neo4j_user = Node("User", id=user_id, username=user["username"], avatar_url=user["avatar_url"], 
                          full_name=user["full_name"], email=user["email"], birthdate=user["birthdate"], 
                          interests=user["interests"], created_at=user["created_at"])
        neo4j_graph.merge(neo4j_user, "User", "id")

def sync_groups():
    """
    Synchronise la collection Groups de MongoDB vers Neo4j sans supprimer les anciens nœuds.
    """
    for group in db.Groups.find():
        group_id = str(group["_id"])  # Convertir ObjectId en chaîne
        neo4j_group = Node("Group", id=group_id, name=group["name"], description=group["description"], 
                           created_by=str(group["created_by"]), created_at=group["created_at"])
        neo4j_graph.merge(neo4j_group, "Group", "id")

def sync_pages():
    """
    Synchronise la collection Pages de MongoDB vers Neo4j sans supprimer les anciens nœuds.
    """
    for page in db.Pages.find():
        page_id = str(page["_id"])  # Convertir ObjectId en chaîne
        neo4j_page = Node("Page", id=page_id, name=page["name"], description=page["description"], 
                          created_by=str(page["created_by"]), created_at=page["created_at"])
        neo4j_graph.merge(neo4j_page, "Page", "id")

def sync_friendships():
    """
    Synchronise les relations d'amitié entre utilisateurs de MongoDB vers Neo4j.
    Exemple: Users1 FRIENDSHIPS Users2
    """
    for user in db.Users.find():
        user_id = str(user["_id"])
        neo4j_user = matcher.match("User", id=user_id).first()
        if neo4j_user:
            for friend_id in user.get("friends", []):
                friend = matcher.match("User", id=str(friend_id)).first()
                if friend:
                    neo4j_graph.merge(Relationship(neo4j_user, "FRIENDS", friend))

def sync_memberships():
    """
    Synchronise les relations de membre entre utilisateurs et groupes de MongoDB vers Neo4j.
    Exemple: Users1 MEMBERSHIPS Groups1
    """
    for user in db.Users.find():
        user_id = str(user["_id"])
        neo4j_user = matcher.match("User", id=user_id).first()
        if neo4j_user:
            for group_id in user.get("groups", []):
                group = matcher.match("Group", id=str(group_id)).first()
                if group:
                    neo4j_graph.merge(Relationship(neo4j_user, "MEMBER_OF", group))

def sync_page_follows():
    """
    Synchronise les relations de suivi entre utilisateurs et pages de MongoDB vers Neo4j.
    Exemple: Users1 FOLLOWS Pages1
    """
    for user in db.Users.find():
        user_id = str(user["_id"])
        neo4j_user = matcher.match("User", id=user_id).first()
        if neo4j_user:
            for page_id in user.get("pages", []):
                page = matcher.match("Page", id=str(page_id)).first()
                if page:
                    neo4j_graph.merge(Relationship(neo4j_user, "FOLLOWS", page))

def full_synchronization():
    """
    Fonction de synchronisation complète qui synchronise tous les utilisateurs, groupes, pages,
    relations d'amitié, relations de membre et relations de suivi de MongoDB vers Neo4j sans supprimer les nœuds existants.
    """
    sync_users()
    sync_groups()
    sync_pages()
    sync_friendships()
    sync_memberships()
    sync_page_follows()
    
    print(f"Synchronisation middleware terminée à {datetime.now()}")

# Planification de la synchronisation quotidienne à minuit
schedule.every().day.at("00:00").do(full_synchronization)

# Boucle pour vérifier s'il est temps de faire la synchronisation
while True:
    print("En attente de la prochaine synchronisation...")
    schedule.run_pending()
    time.sleep(120)
