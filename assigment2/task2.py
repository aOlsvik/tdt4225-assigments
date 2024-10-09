from DbProgram import Program
from tabulate import tabulate

def task2_1(program):
	query = query = """
	SELECT 
		(SELECT COUNT(*) FROM user) AS Users, 
		(SELECT COUNT(*) FROM activity) AS Activities, 
		(SELECT COUNT(*) FROM trackpoint) AS Trackpoints
	"""
	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))

def task2_2(program):
	query = """
	SELECT ROUND(AVG(activity_count), 2) as 'Average activities per user'
	from (
		SELECT COUNT(activity.id) as activity_count
		FROM user
		LEFT JOIN activity ON user.id = activity.user_id
		GROUP BY user.id
	) as activity_counts
	"""

	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))
	

def task2_4(program):
	# Find all users who have taken a taxi 
	# (meaning that the transportation_mode of the activities is 'taxi')
	query = """
	SELECT DISTINCT user.id
	FROM user
	JOIN activity ON user.id = activity.user_id
	WHERE activity.transportation_mode = 'taxi'
	"""
	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))

def task2_6a(program):
	# find the year with the most activities
	query = """
	SELECT YEAR(start_date_time) as year, COUNT(*) as activity_count
	FROM activity
	GROUP BY year
	ORDER BY activity_count DESC
	"""
	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))

def task2_6b(program):
	# find the year with most recorded hours
	query = """
	SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as hours
	FROM activity
	GROUP BY year
	ORDER BY hours DESC
	"""
	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))


def task2_8(program):
	# find the top 20 users who have gained the most altitude meters
	# output should be a table with (id, total meters gained per user)
	# remember that some altitude values are invalid
	user_query = """
	SELECT id FROM user
	"""
	program.cursor.execute(user_query)
	users = program.cursor.fetchall()
	user_altitude_gain = {}
	for user in users:
		user_id = user[0]
		activity_query = f"""
		SELECT id FROM activity WHERE user_id = {user_id}
		"""
		program.cursor.execute(activity_query)
		activities = program.cursor.fetchall()
		total_gain = 0
		# print(activities)
		for activity in activities:
			activity_id = activity[0]

			trackpoint_query = f"""
			SELECT altitude, date_time
			FROM trackpoint
			WHERE activity_id = '{activity_id}' AND altitude > 0
			ORDER BY date_time
			"""
			program.cursor.execute(trackpoint_query)
			trackpoints = program.cursor.fetchall()

			prev_altitude = None
			for point in trackpoints:
				curr_altitude = point[0]
				if curr_altitude != -777:
					if prev_altitude is not None and prev_altitude != -777:
						if curr_altitude > prev_altitude:
							total_gain += (curr_altitude - prev_altitude) * 0.3048
					prev_altitude = curr_altitude
				else:
					continue
		total_gain = int(total_gain)
		user_altitude_gain[user_id] = total_gain
	top_20_users = sorted(user_altitude_gain.items(), key=lambda x: x[1], reverse=True)[:20]
	print(tabulate(top_20_users, headers=['id', 'total meters gained per user']))


def task2_10(program):
	# find the users who have tracked an acitivity in the forbidden city of Beijing
	# In this question you can consider the Forbidden City to have coordinates that correspond to: lat 39.916, lon 116.397
	# user_id is not in the trackpoint table, so we need to join the activity table
	query = """
	SELECT DISTINCT u.id
	FROM user u
	JOIN activity a ON u.id = a.user_id
	JOIN trackpoint t ON a.id = t.activity_id
	WHERE t.lat BETWEEN 39.912 AND 39.920
	AND t.lon BETWEEN 116.394 AND 116.400;
	"""
	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))

def main():
	program = None
	try:
		program = Program()
		# task2_1(program)
		# task2_2(program)
		# task2_4(program)
		# task2_6a(program)
		# task2_6b(program)
		task2_8(program)
		# task2_10(program)
	except Exception as e:
		print("ERROR:", e)
	finally:
		if program:
			program.connection.close_connection()


if __name__ == "__main__":
	main()