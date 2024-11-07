from py2neo import Graph, Node, Relationship, NodeMatcher

# Connexion à Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "rootroot"))
graph.delete_all()

def create_user(id: str, username: str, avatar_url: str, full_name: str, email: str, birthdate: str, interests: list, created_at: str) -> Node:
    """
    Crée un noeud utilisateur dans la bdd neo4j

    Args:
        id (str): Identifiant unique de l'utilisateur
        username (str): Nom d'utilisateur
        avatar_url (str): URL de l'avatar
        full_name (str): Nom complet de l'utilisateur
        email (str): Adresse email de l'utilisateur
        birthdate (str): Date de naissance de l'utilisateur
        interests (list): Liste des intérêts de l'utilisateur
        created_at (str): Date de création de l'utilisateur

    Returns:
        Node: Noeud utilisateur créé
    """
    user = Node("Users", id=id, username=username, avatar_url=avatar_url, full_name=full_name, email=email, birthdate=birthdate, interests=interests, created_at=created_at)
    graph.merge(user, "Users", "id")
    return user

def create_group(id: str, name: str, description: str, created_at: str) -> Node:
    """
    Crée un noeud groupe dans la bdd neo4j

    Args:
        id (str): Identifiant unique du groupe
        name (str): Nom du groupe
        description (str): Description du groupe
        created_at (str): Date de création du groupe

    Returns:
        Node: Noeud groupe créé
    """
    group = Node("Groups", id=id, name=name, description=description, created_at=created_at)
    graph.merge(group, "Groups", "id")
    return group

def create_page(id: str, name: str, description: str, created_at: str) -> Node:
    """
    Crée un noeud page dans la bdd neo4j

    Args:
        id (str): Identifiant unique de la page
        name (str): Nom de la page
        description (str): Description de la page
        created_at (str): Date de création de la page

    Returns:
        Node: Noeud page créé.
    """
    page = Node("Pages", id=id, name=name, description=description, created_at=created_at)
    graph.merge(page, "Pages", "id")
    return page

def create_message(id: str, sender_id: str, receiver_id: str, content: str, created_at: str) -> Node:
    """
    Crée un noeud message dans la bdd neo4j

    Args:
        id (str): Identifiant du message
        sender_id (str): Identifiant de l'émetteur
        receiver_id (str): Identifiant du récepteur
        content (str): Le contenu du message
        created_at (str): Date d'envoi du message
    """
    message = Node("Messages", id=id, sender_id=sender_id, receiver_id=receiver_id, content=content, created_at=created_at)
    graph.merge(message, "Messages", "id")
    return message

def create_friendship(user_id_1: str, user_id_2: str) -> None:
    """
    Crée une relation d'amitié entre deux utilisateurs

    Args:
        user_id_1 (str): ID du premier utilisateur
        user_id_2 (str): ID du second utilisateur
    """
    matcher = NodeMatcher(graph)
    user1 = matcher.match("Users", id=user_id_1).first()
    user2 = matcher.match("Users", id=user_id_2).first()
    if user1 and user2:
        friendship = Relationship(user1, "FRIENDS", user2)
        graph.create(friendship)

def create_membership(user_id: str, group_id: str) -> None:
    """
    Crée une relation de membre entre un utilisateur et un groupe

    Args:
        user_id (str): ID de l'utilisateur
        group_id (str): ID du groupe
    """
    matcher = NodeMatcher(graph)
    user = matcher.match("Users", id=user_id).first()
    group = matcher.match("Groups", id=group_id).first()
    if user and group:
        membership = Relationship(user, "MEMBER_OF", group)
        graph.create(membership)

def create_follow(user_id: str, followed_id: str, followed_type: str = "Pages") -> None:
    """
    Crée une relation de suivi entre un utilisateur et une page ou un autre utilisateur

    Args:
        user_id (str): ID de l'utilisateur
        followed_id (str): ID de l'entité suivie
        followed_type (str): Type de l'entité suivie (par défaut "Pages")
    """
    matcher = NodeMatcher(graph)
    user = matcher.match("Users", id=user_id).first()
    followed = matcher.match(followed_type, id=followed_id).first()
    if user and followed:
        follow = Relationship(user, "FOLLOWS", followed)
        graph.create(follow)

def create_interest(user_id: str, interest: str) -> None:
    """
    Crée une relation d'intérêt entre un utilisateur et un intérêt

    Args:
        user_id (str): ID de l'utilisateur
        interest (str): Intérêt de l'utilisateur
    """
    matcher = NodeMatcher(graph)
    user = matcher.match("Users", id=user_id).first()
    if user:
        interest_node = Node("Interest", name=interest)
        graph.merge(interest_node, "Interest", "name")
        has_interest = Relationship(user, "HAS_INTEREST", interest_node)
        graph.create(has_interest)

# On test la création de noeuds
users = [
    create_user("1", "johndoe", "avatar1.png", "John Doe", "john@example.com", "01-01-1990", ["sports", "music"], "01-11-2024"),
    create_user("2", "janedoe", "avatar2.png", "Jane Doe", "jane@example.com", "02-02-1992", ["reading", "music"], "01-11-2024"),
    create_user("3", "alicesmith", "avatar3.png", "Alice Smith", "alice@example.com", "03-03-1988", ["music", "travel"], "01-11-2024"),
    create_user("4", "bobbrown", "avatar4.png", "Bob Brown", "bob@example.com", "04-04-1995", ["tech"], "01-11-2024"),
    create_user("5", "charliewhite", "avatar5.png", "Charlie White", "charlie@example.com", "05-05-1993", ["tech"], "01-11-2024"),
    create_user("6", "davesmith", "avatar6.png", "Dave Smith", "dave@example.com", "06-06-1994", ["travel"], "01-11-2024")
]

# Création de noeuds groupe
groups = [
    create_group("1", "Music Lovers", "Group for music fans", "01-11-2024"),
    create_group("2", "Tech Enthusiasts", "Group for tech lovers", "01-11-2024"),
    create_group("3", "Travel Buddies", "Group for travel enthusiasts", "01-11-2024")
]

# Création de noeuds page
pages = [
    create_page("1", "Guitar World", "All about guitars", "01-11-2024"),
    create_page("2", "Tech News", "Latest updates in technology", "01-11-2024"),
    create_page("3", "Travel Tips", "Tips for travelers", "01-11-2024")
]

messages = [
    create_message("1", "1", "2", "bonjour jane", "01-11-2024"),
    create_message("2", "2", "1", "bonjour john", "01-11-2024")
]

# Création de relations d'amitié (User 1 est ami avec User 2 et User 3 pour générer des recommandations)
create_friendship("1", "2")
create_friendship("1", "3")

# Création de relations de membre pour les recommandations de groupes
create_membership("2", "1")  # User 2 est membre du groupe Music Lovers
create_membership("3", "3")  # User 3 est membre du groupe Travel Buddies
create_membership("4", "2")  # User 4 est membre du groupe Tech Enthusiasts

# Création de relations de suivi pour les recommandations de pages
create_follow("2", "1", "Pages")  # User 2 suit la page Guitar World
create_follow("3", "3", "Pages")  # User 3 suit la page Travel Tips
create_follow("4", "2", "Pages")  # User 4 suit la page Tech News

# Création de relations d'intérêts pour les intérêts communs
create_interest("1", "music")     # User 1 est intéressé par la musique
create_interest("2", "music")     # User 2 est intéressé par la musique
create_interest("3", "travel")    # User 3 est intéressé par le voyage
create_interest("4", "tech")      # User 4 est intéressé par la technologie