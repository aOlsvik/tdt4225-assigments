from pprint import pprint 
from DbConnector import DbConnector
from haversine import haversine, Unit

db = DbConnector().db

def task2_1():
    # Count users
    num_users = db["user"].count_documents({})
    print("Number of users:", num_users)

    # Count activities
    num_activities = db["activity"].count_documents({})
    print("Number of activities:", num_activities)

    # Count trackpoints
    num_trackpoints = db["trackpoint"].count_documents({})
    print("Number of trackpoints:", num_trackpoints)

def task2_2(): 
    # average activities per user
    query = [
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$group": {"_id": None, "average_activities": {"$avg": "$activity_count"}}}
    ]
    result = list(db["activity"].aggregate(query))
    print("Average activities per user (only counting users with activites):", round(result[0]["average_activities"], 2))
    
    # Calculate total activities and total users
    total_activities = db["activity"].count_documents({})
    total_users = db["user"].count_documents({})

    # Calculate average activities per user including users with zero activities
    avg_activities_per_user = total_activities / total_users if total_users > 0 else 0
    print("Average activities per user (including users with zero activities):", round(avg_activities_per_user, 2))

def task2_3():
    # Find the top 20 users with the most activities
    query = [
        {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 20}
    ]
    top_users = list(db["activity"].aggregate(query))
    pprint(top_users)
    # for user in top_users:
    #     pprint(f"User ID: {user['_id']}, Activities: {user['activity_count']}")

def task2_4():
    # Users who have taken a taxi 
    query = {"transportation_mode": "taxi"}
    users = db["activity"].distinct("user_id", query)
    print("Users who have taken a taxi:")
    pprint(users)

def task2_5():
    # Transportation mode count
    query = [
        {"$match": {"transportation_mode": {"$ne": None}}},
        {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}}
    ]
    t_mode_counts = list(db["activity"].aggregate(query))
    for mode in t_mode_counts:
        pprint(f"Transportation mode: {mode['_id']}, Count: {mode['count']}")

def task2_6a():
    # Year with the most activities
    query = [
        {"$group": {"_id": {"$year": "$start_date_time"}, "activity_count": {"$sum": 1}}},
        {"$sort": {"activity_count": -1}},
        {"$limit": 1}
    ]
    most_active_year = list(db["activity"].aggregate(query))
    print("Year with the most activities:", most_active_year[0]["_id"], "with", most_active_year[0]["activity_count"], "activities")

def task2_6b():
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
    year_most_hours = list(db["activity"].aggregate(query))
    print("Year with the most recorded hours:")
    pprint(year_most_hours[0]["_id"])
    pprint(year_most_hours[0]["total_hours"])

def task2_7():
    # Total distance walked in 2008 by user with id=112
    user_id = "112"
    total_distance = 0.0

    activities_2008 = db["activity"].find({
        "user_id": user_id,
        "transportation_mode": "walk",
        "$expr": {"$eq": [{"$year": "$start_date_time"}, 2008]}
    })
    for activity in activities_2008:
        trackpoints = list(db["trackpoint"].find({"activity_id": activity["_id"]}).sort("date_time", 1))
        for i in range(1, len(trackpoints)):
            # Calculate distance between consecutive trackpoints
            p1 = (trackpoints[i - 1]["lat"], trackpoints[i - 1]["lon"])
            p2 = (trackpoints[i]["lat"], trackpoints[i]["lon"])
            total_distance += haversine(p1, p2, unit=Unit.KILOMETERS)
    print(f"Total distance walked by user {user_id} in 2008: {total_distance:.2f} km")



def task2_8():
    # Top 20 users who have gained the most altitude meters
    # Dictionary to store total altitude gain for each user
    altitude_gain_by_user = {}

    # Loop through each user
    for user in db["user"].find():
        print(f"Processing user: {user}")
        user_id = user["_id"]
        total_altitude_gain = 0.0

        # Define the activity ID prefix based on the user_id (first three digits of activity_id)
        activity_prefix = str(user_id).zfill(3)  # Ensure the prefix is 3 digits, zero-padded if necessary

        # Retrieve activities for this user based on the prefix


        # Step 4: Retrieve and sort trackpoints for the current activity
        trackpoints = db["trackpoint"].find(
            {"activity_id": {"$regex": f"^{activity_prefix}"}, "altitude": {"$ne": -777}}
        ).sort("date_time", 1)

        # Calculate altitude gain for this activity
        activity_altitude_gain = 0.0
        previous_activity_id = None
        for trackpoint in trackpoints:
            if trackpoint["activity_id"] != previous_activity_id:
                previous_altitude = None
            current_altitude = trackpoint["altitude"] * 0.3048  # Convert altitude from feet to meters

            if previous_altitude is not None and current_altitude > previous_altitude:
                activity_altitude_gain += (current_altitude - previous_altitude)

            previous_altitude = current_altitude  # Update previous altitude for the next iteration
            previous_activity_id = trackpoint["activity_id"]

        # Add this activity's altitude gain to the user's total
        total_altitude_gain += activity_altitude_gain

        # Store the total altitude gain for this user
        altitude_gain_by_user[user_id] = total_altitude_gain
        print(f"User ID: {user_id}, Total Altitude Gain: {total_altitude_gain:.2f} meters")

    # Sort users by altitude gain and get the top 20
    top_users = sorted(altitude_gain_by_user.items(), key=lambda x: x[1], reverse=True)[:20]

    # Print results
    for user_id, altitude_gain in top_users:
        print(f"User ID: {user_id}, Altitude Gain: {altitude_gain:.2f} meters")

def main():
    try:
        task2_1()
        # task2_2()
        # task2_3()
        # task2_4()
        # task2_5()
        # task2_6a()
        # task2_6b()
        # task2_7()
        # task2_8()
    except Exception as e:
        print(e)

            
            
if __name__ == "__main__":
    main()