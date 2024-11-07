import schedule
import time
from pymongo import MongoClient
from py2neo import Graph, Node, Relationship, NodeMatcher
from datetime import datetime

# Connexion à la bdd MongoDB
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client.sae5bdd

# Connexion à la bdd Neo4j
neo4j_graph = Graph("bolt://localhost:7687", auth=("neo4j", "rootroot"))
matcher = NodeMatcher(neo4j_graph)

def sync_users():
    """
    Synchronise la collection Users de MongoDB vers Neo4j sans supprimer les anciens noeuds
    """
    for user in db.Users.find():
        user_id = user["id"]
        neo4j_user = Node("Users",
                          id=user_id,
                          username=user["username"],
                          avatar_url=user["avatar_url"], 
                          full_name=user["full_name"],
                          email=user["email"],
                          birthdate=user["birthdate"], 
                          interests=user["interests"],
                          created_at=user["created_at"])
        # Créer le noeud, s'il existe déjà ça met juste à jour les valeurs du noeud existant
        neo4j_graph.merge(neo4j_user, "Users", "id")

def sync_groups():
    """
    Synchronise la collection Groups de MongoDB vers Neo4j sans supprimer les anciens noeuds
    """
    for group in db.Groups.find():
        group_id = group["id"]
        neo4j_group = Node("Groups",
                           id=group_id,
                           name=group["name"],
                           description=group["description"], 
                           created_by=group["created_by"],
                           created_at=group["created_at"])
        # Pareil que dans sync_users()
        neo4j_graph.merge(neo4j_group, "Groups", "id")

def sync_pages():
    """
    Synchronise la collection Pages de MongoDB vers Neo4j sans supprimer les anciens noeuds
    """
    for page in db.Pages.find():
        page_id = page["id"]
        neo4j_page = Node("Pages",
                          id=page_id,
                          name=page["name"],
                          description=page["description"], 
                          created_by=page["created_by"],
                          created_at=page["created_at"])
        neo4j_graph.merge(neo4j_page, "Pages", "id")

def sync_posts():
    """
    Synchronise la collection Posts de MongoDB vers Neo4j et crée les relations entre Users et Posts
    """
    for post in db.Posts.find():
        post_id = post["id"]
        neo4j_post = Node("Posts", id=post_id,
                          content=post["content"],
                          media_url=post["media_url"], 
                          created_at=post["created_at"])
        neo4j_graph.merge(neo4j_post, "Posts", "id")
        
        # Créer la relation "CREATES" entre l'utilisateur et son post
        user_id = post["user_id"]
        neo4j_user = matcher.match("Users", id=user_id).first()
        if neo4j_user:
            neo4j_graph.merge(Relationship(neo4j_user, "CREATES", neo4j_post))

def sync_friendships():
    """
    Synchronise les relations d'amitié entre utilisateurs de MongoDB vers Neo4j.
    """
    for user in db.Users.find():
        user_id = user["id"]
        neo4j_user = matcher.match("Users", id=user_id).first()
        if neo4j_user:
            for friend_id in user.get("friends", []):
                friend = matcher.match("Users", id=friend_id).first()
                if friend:
                    neo4j_graph.merge(Relationship(neo4j_user, "FRIENDS", friend))

def sync_memberships():
    """
    Synchronise les relations de membre entre utilisateurs et groupes de MongoDB vers Neo4j.
    """
    for user in db.Users.find():
        user_id = user["id"]
        neo4j_user = matcher.match("Users", id=user_id).first()
        if neo4j_user:
            for group_id in user.get("groups", []):
                group = matcher.match("Groups", id=group_id).first()
                if group:
                    neo4j_graph.merge(Relationship(neo4j_user, "MEMBER_OF", group))

def sync_page_follows():
    """
    Synchronise les relations de suivi entre utilisateurs et pages de MongoDB vers Neo4j.
    """
    for user in db.Users.find():
        user_id = user["id"]
        neo4j_user = matcher.match("Users", id=user_id).first()
        if neo4j_user:
            for page_id in user.get("pages", []):
                page = matcher.match("Pages", id=page_id).first()
                if page:
                    neo4j_graph.merge(Relationship(neo4j_user, "FOLLOWS", page))

def sync_likes():
    """
    Synchronise les relations "LIKE" entre utilisateurs et posts de MongoDB vers Neo4j.
    """
    for post in db.Posts.find():
        post_id = post["id"]
        neo4j_post = matcher.match("Posts", id=post_id).first()
        if neo4j_post:
            for user_id in post.get("likes", []):
                neo4j_user = matcher.match("Users", id=user_id).first()
                if neo4j_user:
                    neo4j_graph.merge(Relationship(neo4j_user, "LIKES", neo4j_post))

def full_synchronization():
    """
    Fonction qui regroupe toute les synchronisations de la bdd MongoDB vers la bdd neo4j
    """
    sync_users()
    sync_groups()
    sync_pages()
    sync_posts()
    sync_friendships()
    sync_memberships()
    sync_page_follows()
    sync_likes()
    
    print("Synchro middleware: terminé")

# Planification de la synchronisation quotidienne à minuit
schedule.every().day.at("12:51").do(full_synchronization)

# Boucle pour vérifier s'il est l'heure de faire la synchro
while True:
    print("En attente de la prochaine synchronisation...")
    schedule.run_pending()
    time.sleep(5)
