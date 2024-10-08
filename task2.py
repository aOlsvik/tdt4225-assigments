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
	



def main():
	program = None
	try:
		program = Program()
		task2_1(program)
		# task2_2(program)


	except Exception as e:
		print("ERROR:", e)
	finally:
		if program:
			program.connection.close_connection()


if __name__ == "__main__":
	main()