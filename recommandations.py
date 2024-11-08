from flask import Flask, jsonify
from py2neo import Graph

MAX_RECOMMANDATIONS = 5  # On limite à 5 recommandations

app = Flask(__name__)
graph = Graph("bolt://localhost:7687", auth=("neo4j", "rootroot"))

def get_friend_recommendations_by_common_friends(user_id: str, limit: int = MAX_RECOMMANDATIONS) -> list:
    """
    Recommande des amis en fonction du nombre d'amis en commun par ordre décroissant

    Args:
        user_id (str): L'id de l'utilisateur pour lequel on veut les recommandations
        limit (int): Le nombre maximum de recommandations à retourner (par défaut : MAX_RECOMMANDATIONS)

    Retourne:
        list: Liste de dictionnaires contenant les informations des amis recommandés (id utilisateur, nombre d'amis en commun)
    """
    query = f"""
        MATCH (u:Users {{id: "{user_id}"}})-[:FRIENDS]->(friend:Users)-[:FRIENDS]->(recommended)
        WHERE NOT (u)-[:FRIENDS]->(recommended) AND u <> recommended
        WITH recommended, COUNT(friend) AS commonFriends
        RETURN recommended.id AS recommended_user,
               commonFriends
        ORDER BY commonFriends DESC
        LIMIT {limit}
    """
    result = graph.run(query)
    return [
        {
            "user_id": record["recommended_user"],
            "common_friends": record["commonFriends"]
        }
        for record in result
    ]

def get_friend_recommendations_by_common_interests(user_id: str, limit: int = MAX_RECOMMANDATIONS) -> list:
    """
    Recommande des amis en fonction des centres d'intérêt communs

    Args:
        user_id (str): L'id de l'utilisateur pour lequel on veut les recommandations
        limit (int): Le nombre max de recommandations à retourner

    Retourne:
        list: Liste de dictionnaires contenant les informations des amis recommandés (id utilisateur, nombre d'intérêts en commun)
    """
    query = f"""
        MATCH (u:Users {{id: "{user_id}"}})-[:HAS_INTEREST]->(interest)<-[:HAS_INTEREST]-(recommended)
        WHERE NOT (u)-[:FRIENDS]->(recommended) AND u <> recommended
        WITH recommended, COUNT(interest) AS commonInterests
        RETURN recommended.id AS recommended_user,
               commonInterests
        ORDER BY commonInterests DESC
        LIMIT {limit}
    """
    result = graph.run(query)
    return [
        {
            "user_id": record["recommended_user"],
            "common_interests": record["commonInterests"]
        }
        for record in result
    ]

def get_group_recommendations(user_id: str, limit: int = MAX_RECOMMANDATIONS) -> list:
    """
    Recommande des groupes en fonction du nombre d'amis de l'utlisateur présents dans les groupes

    Args:
        user_id (str): L'id de l'utilisateur pour lequel on veut les recommandations
        limit (int): Le nombre maximum de recommandations à retourner

    Retourne:
        list: Liste de dictionnaires contenant les informations des groupes recommandés (id du groupe, nom, nombre d'amis dans le groupe)
    """
    query = f"""
        MATCH (u:Users {{id: "{user_id}"}})-[:FRIENDS]->(friend:Users)-[:MEMBER_OF]->(group:Groups)
        WHERE NOT (u)-[:MEMBER_OF]->(group)
        RETURN group.id AS group_id,
               group.name AS group_name,
               COUNT(friend) AS friends_in_group
        ORDER BY friends_in_group DESC
        LIMIT {limit}
    """
    result = graph.run(query)
    return [
        {
            "group_id": record["group_id"],
            "group_name": record["group_name"],
            "friends_in_group": record["friends_in_group"]
        }
        for record in result
    ]

def get_page_recommendations(user_id: str, limit: int = MAX_RECOMMANDATIONS) -> list:
    """
    Recommande des pages en fonction du nombre d'amis de l'utlisateur qui suivent les pages

    Args:
        user_id (str): L'id de l'utilisateur pour lequel on veut les recommandations
        limit (int): Le nombre maximum de recommandations à retourner

    Retourne:
        list: Liste de dictionnaires contenant les informations des pages recommandées (id de la page, nom, nombre d'amis qui suivent la page)
    """
    query = f"""
        MATCH (u:Users {{id: "{user_id}"}})-[:FRIENDS]->(friend:Users)-[:FOLLOWS]->(page:Pages)
        WHERE NOT (u)-[:FOLLOWS]->(page)
        RETURN page.id AS page_id,
               page.name AS page_name,
               COUNT(friend) AS friends_following_page
        ORDER BY friends_following_page DESC
        LIMIT {limit}
    """
    result = graph.run(query)
    return [
        {
            "page_id": record["page_id"],
            "page_name": record["page_name"],
            "friends_following_page": record["friends_following_page"]
        }
        for record in result
    ]

@app.route('/recommendations/<user_id>', methods=['GET'])
def recommend(user_id: str):
    """
    Lien de l'API pour récupérer toute les recommandations d'un utilisateur

    Args:
        user_id (str): L'id de l'utilisateur pour lequel on veut les recommandations

    Retourne:
        Response: Objet JSON qui contient les recommandations organisées par amis (en communs, centre d'intérêts), groupes et pages
    """
    friends_by_common_friends = get_friend_recommendations_by_common_friends(user_id)
    friends_by_common_interests = get_friend_recommendations_by_common_interests(user_id)
    group_recommendations = get_group_recommendations(user_id)
    page_recommendations = get_page_recommendations(user_id)

    response = {
        "user_id": user_id,
        "recommended_friends": {
            "by_common_friends": friends_by_common_friends,
            "by_common_interests": friends_by_common_interests
        },
        "recommended_groups": {
            "count": len(group_recommendations),
            "details": group_recommendations
        },
        "recommended_pages": {
            "count": len(page_recommendations),
            "details": page_recommendations
        }
    }
    
    return jsonify(response)

# Lancer le serveur Flask
if __name__ == '__main__':
    app.run(port=5001, debug=True)
