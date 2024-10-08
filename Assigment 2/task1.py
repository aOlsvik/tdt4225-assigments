from DbConnector import DbConnector
from tabulate import tabulate
from table_create import getTableQueries
import os
from DbProgram import Program

def insert_users(program):
    max = 182
    labeled_ids = open("dataset/labeled_ids.txt", "r")
    labeled_ids = labeled_ids.readlines()
    labeled_ids = [id.strip() for id in labeled_ids]
    ids = [f"{i:03}" for i in range(max)]
    insert_ids = [(id, int(id in labeled_ids)) for id in ids]
    insert_user_query = "INSERT INTO user (id, has_labels) VALUES (%s, %s)"
    program.cursor.executemany(insert_user_query, insert_ids)
    program.db_connection.commit()
    return insert_ids

def create_tables(program):
    table_queries = getTableQueries()
    for table_name, query in table_queries.items():
        program.create_table(query)


def insert_activities_labeled(program, labeled_ids):
    insert_activity_query = "INSERT INTO activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)"
    labeled_activites = {}
    insert_activites = []

    for user_id in labeled_ids:
        activity_file = f"dataset/Data/%s/labels.txt" % user_id
        activities = open(activity_file, "r")
        activities = activities.readlines()
        activities = activities[1:]
        user_activities = []
        for i, activity in enumerate(activities):
            insert_id = user_id + '_' + str(i)
            activity = activity.split('\t')

            start_date_time = activity[0].strip().replace('/', '-')
            end_date_time = activity[1].strip().replace('/', '-')
            transportation_mode = activity[2].strip()
            user_activities.append((insert_id, user_id, transportation_mode, start_date_time, end_date_time))

        insert_activites.extend(user_activities)
        labeled_activites[user_id] = user_activities

    program.cursor.executemany(insert_activity_query, insert_activites)
    program.db_connection.commit()

    return labeled_activites

def insert_trackpoints(program, ids, insert_activites_labeled):
    insert_activity_query = "INSERT INTO activity (id, user_id, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)" 
    insert_trackpoint_query = "INSERT INTO trackpoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
    valid_activities = set()

    # unlabeled activites to be insterted
    insert_activities_unlabeled = []

    # trackpoints to be inserted
    insert_trackpoints = []

    for id, label in ids:
        folder = "dataset/Data/%s" % id
        files = os.listdir(folder + "/Trajectory")
        for i, file in enumerate(files):
            # read file and check length
            lines = open(folder + "/Trajectory/" + file, "r")
            lines = lines.readlines()
            lines = lines[6:]
            if len(lines) > 2500:
                continue

            # find start and end time
            first_line = lines[0].split(',')
            last_line = lines[-1].split(',')
            start_date = first_line[5] + ' ' + first_line[6]
            end_date = last_line[5] + ' ' + last_line[6]
            start_date = start_date.strip()
            end_date = end_date.strip()

            # create activity if not labeled with matchin start and end date
            activity_id = None
            if label == 0:
                activity_id = id + '_' + str(i)
                insert_activities_unlabeled.append((activity_id, id, start_date, end_date))

            # check if start and end date matches labeled activity
            else:
                matches = [activity for activity in insert_activites_labeled[id] if activity[3] == start_date and activity[4] == end_date]
                if len(matches) == 0 or len(matches) > 1:
                    continue
                activity_id = matches[0][0]

            # since some activites wont have matches, add the ones that do to the valid_activities set
            valid_activities.add(activity_id)

            # insert trackpoints
            for line in lines:
                line = line.split(',')
                line = [l.strip() for l in line]
                lat = line[0]
                lon = line[1]
                altitude = line[3]
                date_days = line[4]
                date_time = line[5] + ' ' + line[6]
                insert_trackpoints.append((activity_id, lat, lon, altitude, date_days, date_time))
    
    print("Inserting unlabeled activities...")
    print("Amount of unlabeled activities to be inserted: ", len(insert_activities_unlabeled))
    print("Total amount of activites: ", len(valid_activities))
    program.cursor.executemany(insert_activity_query, insert_activities_unlabeled)
    program.db_connection.commit()
    print("Inserting trackpoints...")
    print("Amount of trackpoints to be inserted: ", len(insert_trackpoints))
    batch_size = 10000
    for i in range(0, len(insert_trackpoints), batch_size):
        if i % 10**6 == 0:
            print(f"Inserted {i} trackpoints")
        program.cursor.executemany(insert_trackpoint_query, insert_trackpoints[i:i+batch_size])
        program.db_connection.commit()
            
    return valid_activities


def main():
    program = None
    try:
        # connect
        program = Program()
        print("Dropping tables...")
        program.drop_table("trackpoint")
        program.drop_table("activity")
        program.drop_table("user")

        print("Creating tables...")
        create_tables(program)
        program.show_tables()

        print("Inserting users...")
        ids = insert_users(program)
        
        print("Inserting labeled activities...")
        labeled_ids = [id for id, label in ids if label == 1]
        activites = insert_activities_labeled(program, labeled_ids)

        print("Inserting trackpoints...")
        valid_activites = insert_trackpoints(program, ids, activites)

        print("Removing invalid activities...")
        tuple_ids = tuple(valid_activites)
        program.cursor.execute("DELETE FROM activity WHERE id NOT IN %s" % str(tuple_ids))
        program.db_connection.commit()


    except Exception as e:
        print("ERROR:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
