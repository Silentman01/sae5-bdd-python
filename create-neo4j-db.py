from py2neo import Graph, Node, Relationship, NodeMatcher

# Connexion à Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "rootroot"))

# Fonction pour créer un nœud utilisateur
def create_user(id, username, avatar_url, full_name, email, birthdate, interests, created_at):
    user = Node("User", id=id, username=username, avatar_url=avatar_url, 
                full_name=full_name, email=email, birthdate=birthdate, 
                interests=interests, created_at=created_at)
    graph.merge(user, "User", "id")
    return user

# Fonction pour créer un nœud groupe
def create_group(id, name, description, created_at):
    group = Node("Group", id=id, name=name, description=description, created_at=created_at)
    graph.merge(group, "Group", "id")
    return group

# Fonction pour créer un nœud page
def create_page(id, name, description, created_at):
    page = Node("Page", id=id, name=name, description=description, created_at=created_at)
    graph.merge(page, "Page", "id")
    return page

# Fonction pour créer des relations (d'amitié, de membre, de suivi, d'intérêt)
def create_friendship(user_id_1, user_id_2):
    matcher = NodeMatcher(graph)
    user1 = matcher.match("User", id=user_id_1).first()
    user2 = matcher.match("User", id=user_id_2).first()
    if user1 and user2:
        friendship = Relationship(user1, "FRIENDS", user2)
        graph.create(friendship)

def create_membership(user_id, group_id):
    matcher = NodeMatcher(graph)
    user = matcher.match("User", id=user_id).first()
    group = matcher.match("Group", id=group_id).first()
    if user and group:
        membership = Relationship(user, "MEMBER_OF", group)
        graph.create(membership)

def create_follow(user_id, followed_id, followed_type="Page"):
    matcher = NodeMatcher(graph)
    user = matcher.match("User", id=user_id).first()
    followed = matcher.match(followed_type, id=followed_id).first()
    if user and followed:
        follow = Relationship(user, "FOLLOWS", followed)
        graph.create(follow)

def create_interest(user_id, interest):
    matcher = NodeMatcher(graph)
    user = matcher.match("User", id=user_id).first()
    if user:
        interest_node = Node("Interest", name=interest)
        graph.merge(interest_node, "Interest", "name")
        has_interest = Relationship(user, "HAS_INTEREST", interest_node)
        graph.create(has_interest)

# Création de nœuds utilisateur
users = [
    create_user("1", "johndoe", "avatar1.png", "John Doe", "john@example.com", "1990-01-01", ["sports", "music"], "2023-01-01"),
    create_user("2", "janedoe", "avatar2.png", "Jane Doe", "jane@example.com", "1992-02-02", ["reading", "music"], "2023-01-02"),
    create_user("3", "alicesmith", "avatar3.png", "Alice Smith", "alice@example.com", "1988-03-03", ["music", "travel"], "2023-01-03"),
    create_user("4", "bobbrown", "avatar4.png", "Bob Brown", "bob@example.com", "1995-04-04", ["tech"], "2023-01-04"),
    create_user("5", "charliewhite", "avatar5.png", "Charlie White", "charlie@example.com", "1993-05-05", ["tech"], "2023-01-05"),
    create_user("6", "davesmith", "avatar6.png", "Dave Smith", "dave@example.com", "1994-06-06", ["travel"], "2023-01-06")
]

# Création de nœuds groupe
groups = [
    create_group("1", "Music Lovers", "Group for music fans", "2023-01-01"),
    create_group("2", "Tech Enthusiasts", "Group for tech lovers", "2023-01-02"),
    create_group("3", "Travel Buddies", "Group for travel enthusiasts", "2023-01-03")
]

# Création de nœuds page
pages = [
    create_page("1", "Guitar World", "All about guitars", "2023-01-01"),
    create_page("2", "Tech News", "Latest updates in technology", "2023-01-02"),
    create_page("3", "Travel Tips", "Tips for travelers", "2023-01-03")
]

# Création de relations d'amitié (User 1 est ami avec User 2 et User 3 pour générer des recommandations)
create_friendship("1", "2")
create_friendship("1", "3")

# Création de relations de membre pour les recommandations de groupes
create_membership("2", "1")  # User 2 est membre du groupe Music Lovers
create_membership("3", "3")  # User 3 est membre du groupe Travel Buddies
create_membership("4", "2")  # User 4 est membre du groupe Tech Enthusiasts

# Création de relations de suivi pour les recommandations de pages
create_follow("2", "1", "Page")  # User 2 suit la page Guitar World
create_follow("3", "3", "Page")  # User 3 suit la page Travel Tips
create_follow("4", "2", "Page")  # User 4 suit la page Tech News

# Création de relations d'intérêts pour les intérêts communs
create_interest("1", "music")     # User 1 est intéressé par la musique
create_interest("2", "music")     # User 2 est intéressé par la musique
create_interest("3", "travel")    # User 3 est intéressé par le voyage
create_interest("4", "tech")      # User 4 est intéressé par la technologie