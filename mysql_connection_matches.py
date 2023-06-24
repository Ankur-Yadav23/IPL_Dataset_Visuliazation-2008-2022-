import csv
import mysql.connector

# MySQL database connection details
db_host = 'localhost'
db_user = 'root'
db_password = 'ankur'
db_name = 'IPL_DATABASE'

# CSV file path
csv_file = 'D:/Ankur/VITA DBDA/Data Visualization/IPL/IPL_MATCHES_DATA.csv'

# MySQL table details
table_name = 'IPL_MATCHES_DATA'

# Establish a connection to the MySQL database
db_connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

# Create a cursor object to interact with the database
cursor = db_connection.cursor()

# Open the CSV file and read its contents
with open(csv_file, 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip the header row if needed

    # Iterate through the CSV data and insert into the MySQL table
    for row in csv_data:
        query = f"INSERT INTO {table_name} (id, city, match_date, season, match_number, team1, team2, venue, toss_winner, toss_decision, superover, winning_team, won_by, margin, method, player_of_match, umpire1, umpire2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = tuple(row)
        cursor.execute(query, values)

# Commit the changes and close the database connection
db_connection.commit()
cursor.close()
db_connection.close()

print("CSV data successfully imported into MySQL table.")
