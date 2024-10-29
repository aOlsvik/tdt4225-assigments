from Program import Program
from pprint import pprint 
import os


def insert_users(program):
    labeled_ids = open("./dataset/labeled_ids.txt", "r")
    labeled_ids = labeled_ids.readlines()
    labeled_ids = [id.strip() for id in labeled_ids]
    ids = [f"{i:03}" for i in range(182)]
    insert_ids = [(id, int(id in labeled_ids)) for id in ids]
    insert_users = [
        {
            '_id': user_id,
            'labeled': labeled_data
        } 
                    for user_id, labeled_data in insert_ids]
    
    program.insert_documents(collection_name='user', docs=insert_users)
    return insert_users
    
def insert_activities_and_trackpoints(program, inserted_users):
    # logic:
    # loop through all users
    # loop through trackpoints, creating activities
    # insert activities
    
    for user in inserted_users:
        insert_activites = []
        insert_trackpoints = [] 
        
        dir = f"./dataset/Data/{user['_id']}"
        
        files = os.listdir(f"{dir}/Trajectory")
        
        for index, file in enumerate(files):
            with open(f"{dir}/Trajectory/{file}", "r") as f:
                lines = f.readlines()
                lines = lines[6:]
                if len(lines) > 2500:
                    continue
                lines = [line.strip().split(",") for line in lines]
                activity_id = f"{user['_id']}_{index}"
                insert_activites.append({
                    '_id': activity_id,
                    'user_id': user['_id'],
                    'start_date_time': lines[0][5] + " " + lines[0][6],
                    'end_date_time': lines[-1][5] + " " + lines[-1][6]
                })
                trackpoints = [
                    {
                        'lat': float(line[0]),
                        'lon': float(line[1]),
                        'altitude': float(line[3]),
                        'datetime': line[5] + " " + line[6],
                        'activity_id': activity_id
                    } for line in lines
                ]
                insert_trackpoints.extend(trackpoints)
                
        if user['labeled'] == 1:
            with open(f"{dir}/labels.txt", "r") as f:
                lines = f.readlines()
                lines = lines[1:]
                for index, line in enumerate(lines):
                    start_time = line.split("\t")[0].replace("/", "-")
                    end_time = line.split("\t")[1].replace("/", "-")
                    mode = line.split("\t")[2].strip()
                    for activity in insert_activites:
                        if activity['start_date_time'] == start_time and activity['end_date_time'] == end_time:
                            activity['transportation_mode'] = mode
                            break
                        
        print(f"Inserting {len(insert_activites)} activities and {len(insert_trackpoints)} trackpoints for user {user['_id']}")
        
        if len(insert_activites) > 0:
            program.insert_documents(collection_name='activity', docs=insert_activites)
            program.insert_documents(collection_name='trackpoint', docs=insert_trackpoints)
        

def main():
    program = None
    try:
        program = Program()
        cols = ['user', 'activity', 'trackpoint']
        for col in cols:
            program.drop_coll(collection_name=col)
        for col in cols:
            program.create_coll(collection_name=col)
   
        print("Inserting users...")
        inserted_users = insert_users(program)
        
        print("Inserting activities and trackpoints...")
        insert_activities_and_trackpoints(program, inserted_users)
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()
    


if __name__ == "__main__":
    main()