from pprint import pprint 
from DbConnector import DbConnector
from haversine import haversine, Unit

def task2_1(program):
    # Count users
    num_users = db["User"].count_documents({})
    pprint("Number of users:", num_users)

    # Count activities
    num_activities = db["Activity"].count_documents({})
    pprint("Number of activities:", num_activities)

    # Count trackpoints
    num_trackpoints = db["TrackPoint"].count_documents({})
    pprint("Number of trackpoints:", num_trackpoints)

def task2_2(program): 
    # average activities per user
    query = [
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$group": {"_id": None, "average_activities": {"$avg": "$activity_count"}}}
    ]
    result = list(db["Activity"].aggregate(query))
    pprint("Average activities per user:", result[0]["average_activities"])

def task2_3(program):
    # Find the top 20 users with the most activities
    query = [
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 20}
    ]
    top_users = list(db["Activity"].aggregate(query))
    for user in top_users:
        pprint(f"User ID: {user['_id']}, Activities: {user['activity_count']}")

def task2_4(program):
    # Users who have taken a taxi 
    query = {"transportation_mode": "taxi"}
    users = db["Activity"].distinct("user_id", query)
    pprint("Users who have taken a taxi:", users)

def task2_5(program):
    # Transportation mode count
    query = [
        {"$match": {"transportation_mode": {"$ne": None}}},
        {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}}
    ]
    t_mode_counts = list(db["Activity"].aggregate(query))
    for mode in t_mode_counts:
        pprint(f"Transportation mode: {mode['_id']}, Count: {mode['count']}")

def task2_6a(program):
    # Year with the most activities
    query = [
        {"$group": {"_id": {"$year": "$start_date_time"}, "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 1}
    ]
    most_active_year = list(db["Activity"].aggregate(query))
    pprint("Year with the most activities:", most_active_year[0]["_id"], "with", most_active_year[0]["activity_count"], "activities")

def task2_6b(program):
    # year with most recorded hours
    query = [
        {"$project": {
            "year": {"$year": "$start_date_time"},
            "duration_hours": {
                "$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]
            }
        }},
        {"$group": {"_id": "$year", "total_hours": {"$sum": "$duration_hours"}}},
        {"$sort": {"total_hours": -1}},
        {"$limit": 1}
    ]
    year_most_hours = list(db["Activity"].aggregate(query))
    pprint("Year with the most recorded hours:", year_most_hours[0]["_id"], "with", year_most_hours[0]["total_hours"], "hours")

def task2_7(program):
    # Total distance walked in 2008 by user with id=112
    user_id = "112"
    total_distance = 0.0

    activities_2008 = db["Activity"].find({
        "user_id": user_id,
        "transportation_mode": "walk",
        "$expr": {"$eq": [{"$year": "$start_date_time"}, 2008]}
    })
    for activity in activities_2008:
    trackpoints = list(db["TrackPoint"].find({"activity_id": activity["_id"]}).sort("date_time", 1))
    for i in range(1, len(trackpoints)):
        # Step 3: Calculate distance between consecutive trackpoints
        p1 = (trackpoints[i - 1]["lat"], trackpoints[i - 1]["lon"])
        p2 = (trackpoints[i]["lat"], trackpoints[i]["lon"])
        total_distance += haversine(p1, p2, unit=Unit.KILOMETERS)