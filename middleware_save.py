import schedule
import time
from pymongo import MongoClient
from py2neo import Graph, Node, Relationship, NodeMatcher
from bson.binary import Binary
from datetime import datetime

# Connexion à la bdd MongoDB
mongo_client = MongoClient("mongodb://localhost:27017")
# test database
# db = mongo_client.sae5bdd
db = mongo_client.socialnetworkdb

# Connexion à la bdd Neo4j
neo4j_graph = Graph("bolt://localhost:7687", auth=("neo4j", "rootroot"))
matcher = NodeMatcher(neo4j_graph)

def binary_id_to_str(id):
    """
    Convertit un champ de type Binary en chaîne de caractères.
    """
    if isinstance(id, Binary):
        return id.hex()
    return str(id)

def sync_users():
    """
    Synchronise la collection users de MongoDB vers Neo4j sans supprimer les anciens noeuds
    """
    for user in db.users.find():
        user_id = binary_id_to_str(user["_id"])

        neo4j_user = Node("Users",
                          id=user_id,
                          username=user["username"],
                          avatar=user["avatar"],
                          bio=user["bio"],
                          interests=user["interests"],
                          first_name=user["first_name"],
                          last_name=user["last_name"],
                          mail=user["mail"],
                          password=user["password"],
                          role=user["role"],
                          birthdate=user["birthdate"],
                          createdAt=user["createdAt"])
        # Créer le noeud, s'il existe déjà ça met juste à jour les valeurs du noeud existant
        neo4j_graph.merge(neo4j_user, "Users", "id")
        
    print("Synch users: terminé")

def sync_groups():
    """
    Synchronise la collection group de MongoDB vers Neo4j sans supprimer les anciens noeuds
    """
    for group in db.group.find():
        group_id = binary_id_to_str(group["_id"])
        if len(group["createdBy"]) > 0:
            createdBy_id = binary_id_to_str(group["createdBy"]["_id"])
        else:
            createdBy_id = "None"
        neo4j_group = Node("Group",
                           id=group_id,
                           name=group["name"],
                           description=group["description"], 
                           createdBy=createdBy_id,
                           createdAt=group["createdAt"])
        # Pareil que dans sync_users()
        neo4j_graph.merge(neo4j_group, "Group", "id")
        
    print("Synch group: terminé")

def sync_pages():
    """
    Synchronise la collection pages de MongoDB vers Neo4j sans supprimer les anciens noeuds
    """
    for page in db.pages.find():
        page_id = binary_id_to_str(page["_id"])
        if len(page["createdBy"]) > 0:
            createdBy_id = binary_id_to_str(page["createdBy"]["_id"])
        else:
            createdBy_id = "None"
        neo4j_page = Node("Pages",
                          id=page_id,
                          name=page["name"],
                          description=page["description"], 
                          createdBy=createdBy_id,
                          createdAt=page["createdAt"])
        neo4j_graph.merge(neo4j_page, "Pages", "id")

        # Créer la relation "CREATED_PAGES" entre l'utilisateur et son post
        userId = createdBy_id
        neo4j_user = matcher.match("Users", id=userId).first()
        if neo4j_user:
            neo4j_graph.merge(Relationship(neo4j_user, "CREATED_PAGE", neo4j_page))

    print("Synch pages: terminé")

def sync_posts():
    """
    Synchronise la collection posts de MongoDB vers Neo4j et crée les relations entre Users et Posts
    """
    for post in db.posts.find():
        post_id = binary_id_to_str(post["_id"])
        neo4j_post = Node("Posts",
                          id=post_id,
                          content=post["content"],
                          image=post["image"], 
                          createdAt=post["createdAt"])
        neo4j_graph.merge(neo4j_post, "Posts", "id")
        
        # Créer la relation "CREATES" entre l'utilisateur et son post
        userId = binary_id_to_str(post["userId"])
        neo4j_user = matcher.match("Users", id=userId).first()
        if neo4j_user:
            neo4j_graph.merge(Relationship(neo4j_user, "POSTED", neo4j_post))
        
    print("Synch posts: terminé")

def sync_privates_messages():
    """
    Synchronise la collection privates_messages de MongoDB vers neo4j et créé les relations entre l'émetteur et le récepteur
    """
    for privateMessage in db.privates_messages.find():
        privateMessage_id = binary_id_to_str(privateMessage["_id"])
        neo4j_privateMessage = Node("PrivateMessage",
                                    id=privateMessage_id,
                                    content=privateMessage["content"],
                                    createdAt=privateMessage["createdAt"]
                                    )
        neo4j_graph.merge(neo4j_privateMessage, "PrivateMessage", "id")

        # Créer la relation "SEND_MESSAGE" et "RECEIVE_MESSAGE" entre les deux utilisateurs et le message
        sender_id = binary_id_to_str(privateMessage["sender_id"])
        receiver_id = binary_id_to_str(privateMessage["receiver_id"])
        neo4j_sender = matcher.match("Users", id=sender_id).first()
        neo4j_receiver = matcher.match("Users", id=receiver_id).first()
        if neo4j_sender and neo4j_receiver:
            neo4j_graph.merge(Relationship(neo4j_sender, "SEND_MESSAGE", neo4j_privateMessage))
            neo4j_graph.merge(Relationship(neo4j_privateMessage, "RECEIVE_MESSAGE", neo4j_receiver))
        
    print("Synch private messages: terminé")

def sync_friendships():
    """
    Synchronise les relations d'amitié entre les Users de MongoDB vers Neo4j
    """
    for user in db.users.find():
        user_id = binary_id_to_str(user["_id"])
        neo4j_user = matcher.match("Users", id=user_id).first()
        if neo4j_user:
            for friend_id in user.get("friends", []):
                friend = matcher.match("Users", id=friend_id).first()
                if friend:
                    neo4j_graph.merge(Relationship(neo4j_user, "FRIENDS", friend))
        
    print("Synch FRIENDS: terminé")

def sync_memberships():
    """
    Synchronise les relations de membre entre users et group de MongoDB vers neo4j
    """
    for user in db.users.find():
        user_id = binary_id_to_str(user["_id"])
        neo4j_user = matcher.match("Users", id=user_id).first()
        if neo4j_user:
            for group_id in user.get("groups", []):
                group = matcher.match("Group", id=group_id).first()
                if group:
                    neo4j_graph.merge(Relationship(neo4j_user, "MEMBER_OF", group))
        
    print("Synch MEMBER_OF: terminé")

def sync_page_follows():
    """
    Synchronise les relations de suivi entre users et pages de MongoDB vers Neo4j
    """
    for user in db.users.find():
        user_id = binary_id_to_str(user["_id"])
        neo4j_user = matcher.match("Users", id=user_id).first()
        if neo4j_user:
            for page_id in user.get("pages", []):
                page_id = binary_id_to_str(page_id)
                page = matcher.match("Pages", id=page_id).first()
                if page:
                    neo4j_graph.merge(Relationship(neo4j_user, "FOLLOWS", page))
        
    print("Synch FOLLOWS: terminé")

def sync_likes():
    """
    Synchronise les relations j'aime entre Users et Posts de MongoDB vers Neo4j
    """
    for post in db.posts.find():
        post_id = binary_id_to_str(post["_id"])
        neo4j_post = matcher.match("Posts", id=post_id).first()
        if neo4j_post:
            for user_id in post.get("likes", []):
                neo4j_user = matcher.match("Users", id=user_id).first()
                if neo4j_user:
                    neo4j_graph.merge(Relationship(neo4j_user, "LIKES", neo4j_post))
        
    print("Synch LIKES: terminé")

def full_synchronization():
    """
    Fonction qui regroupe toute les synchronisations de la bdd MongoDB vers la bdd neo4j
    """
    sync_users()
    sync_groups()
    sync_pages()
    sync_posts()
    sync_privates_messages()
    sync_friendships()
    sync_memberships()
    sync_page_follows()
    sync_likes()
    
    print("Synchro middleware: terminé")

full_synchronization()

"""# Planification de la synchronisation quotidienne à minuit
schedule.every().day.at("09:40").do(full_synchronization)

# Boucle pour vérifier s'il est l'heure de faire la synchro
while True:
    print("En attente de la prochaine synchronisation...")
    schedule.run_pending()
    time.sleep(5)
"""