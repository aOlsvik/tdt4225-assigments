from DbProgram import Program
from tabulate import tabulate
from haversine import haversine, Unit

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
	
def task2_3(program):
		query = """
		SELECT user.id User, COUNT(activity.id) Activities from user
		LEFT JOIN activity ON user.id = activity.user_id
		GROUP BY user.id
		ORDER BY COUNT(activity.id) DESC
		LIMIT 20
		"""
		program.cursor.execute(query)
		rows = program.cursor.fetchall()
		print(tabulate(rows, headers=program.cursor.column_names))


def task2_5(program):
	query = """
	SELECT transportation_mode, COUNT(*) as count FROM activity
	where transportation_mode IS NOT NULL
	GROUP BY transportation_mode
	UNION
	SELECT 'SUM', COUNT(*) FROM activity
	where transportation_mode IS NOT NULL
	"""

	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))


def task2_7(program):
	query = """
	SELECT activity_id, lat, lon FROM trackpoint
	JOIN activity ON trackpoint.activity_id = activity.id
	WHERE activity.user_id = '112'
	AND activity.transportation_mode = 'walk'
	AND activity.start_date_time >= '2008-01-01 00:00:00'
	AND activity.end_date_time <= '2008-12-31 23:59:59'
	"""

	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	
	activities = {}
	for row in rows:
		activity_id = row[0]
		if activity_id not in activities:
			activities[activity_id] = []
		lat = row[1]
		lon = row[2]
		activities[activity_id].append((lat, lon))
	
	total_distance = 0
	for activity_id, trackpoints in activities.items():
		for i in range(1, len(trackpoints)):
			total_distance += haversine(trackpoints[i-1], trackpoints[i], unit=Unit.KILOMETERS)

	print(f"Total distance walked by user 112 in 2008: ", round(total_distance, 2), "km")


def task2_9(program):
	subquery = """
	SELECT a.user_id, tp.activity_id FROM activity a
	JOIN trackpoint tp ON a.id = tp.activity_id 
	JOIN trackpoint tp2 ON tp.id = tp2.id-1 
	WHERE tp.activity_id = tp2.activity_id
	AND TIMESTAMPDIFF(MINUTE, tp.date_time, tp2.date_time) > 5
	"""
	query = f"""
	SELECT user_id, COUNT(DISTINCT activity_id) FROM ({subquery}) AS subquery
	GROUP BY user_id
	"""
	program.cursor.execute(query)
	rows = program.cursor.fetchall()
	print(tabulate(rows, headers=program.cursor.column_names))

def task2_11(program):
	query = """
	SELECT max_counts.user_id, max_counts.transportation_mode_count, counts.transportation_mode 
	FROM (
		SELECT user_id, MAX(transportation_mode_count) AS transportation_mode_count
		FROM (
				SELECT user_id, COUNT(*) AS transportation_mode_count
				FROM activity
				WHERE transportation_mode IS NOT NULL
				GROUP BY user_id, transportation_mode
			) AS counts
		GROUP BY user_id
	) as max_counts
	JOIN (
			SELECT user_id, COUNT(transportation_mode) as transportation_mode_count, transportation_mode
			FROM activity
			WHERE transportation_mode IS NOT NULL
			GROUP BY user_id, transportation_mode
	) as counts ON max_counts.user_id = counts.user_id
	WHERE max_counts.transportation_mode_count = counts.transportation_mode_count
	ORDER BY max_counts.user_id ASC
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
		# task2_3(program)
		# task2_5(program)
		# task2_7(program)
		# task2_9(program)
		# task2_11(program)


	except Exception as e:
		print("ERROR:", e)
	finally:
		if program:
			program.connection.close_connection()


if __name__ == "__main__":
	main()