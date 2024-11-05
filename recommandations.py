from flask import Flask, jsonify
from py2neo import Graph

MAX_RECOMMANDATIONS = 5

app = Flask(__name__)
graph = Graph("bolt://localhost:7687", auth=("neo4j", "rootroot"))

def get_friend_recommendations_by_common_friends(user_id, limit=MAX_RECOMMANDATIONS):
    query = f"""
        MATCH (u:User {{id: "{user_id}"}})-[:FRIENDS]->(friend:User)-[:FRIENDS]->(recommended)
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

def get_friend_recommendations_by_common_interests(user_id, limit=MAX_RECOMMANDATIONS):
    query = f"""
        MATCH (u:User {{id: "{user_id}"}})-[:HAS_INTEREST]->(interest)<-[:HAS_INTEREST]-(recommended)
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

def get_group_recommendations(user_id, limit=MAX_RECOMMANDATIONS):
    query = f"""
        MATCH (u:User {{id: "{user_id}"}})-[:FRIENDS]->(friend:User)-[:MEMBER_OF]->(group:Group)
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

def get_page_recommendations(user_id, limit=MAX_RECOMMANDATIONS):
    query = f"""
        MATCH (u:User {{id: "{user_id}"}})-[:FRIENDS]->(friend:User)-[:FOLLOWS]->(page:Page)
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
def recommend(user_id):
    # Collecter toutes les recommandations d'amis, groupes et pages
    friends_by_common_friends = get_friend_recommendations_by_common_friends(user_id)
    friends_by_common_interests = get_friend_recommendations_by_common_interests(user_id)
    group_recommendations = get_group_recommendations(user_id)
    page_recommendations = get_page_recommendations(user_id)

    # Structure de réponse pour toutes les recommandations
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
