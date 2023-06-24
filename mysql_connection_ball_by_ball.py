import csv
import mysql.connector

# MySQL database connection details
db_host = 'localhost'
db_user = 'root'
db_password = 'ankur'
db_name = 'IPL_DATABASE'

# CSV file path
csv_file = 'D:/Ankur/VITA DBDA/Data Visualization/IPL/IPL_BALL_BY_BALL_DATA.csv'

# MySQL table details
table_name = 'IPL_BALL_BY_BALL_DATA'

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

    # Fetch and insert 225,955 rows from the CSV file
    row_count = 0
    batch_size = 1000  # Adjust the batch size as per your system's resources

    for row in csv_data:
        query = f"INSERT INTO {table_name} (id, innings, overs, ball_number, batter, bowler, non_striker, extra_type, batsman_run, extras_run, total_run, non_boundary, iswicket_delivery, player_out, dismissal_kind, fielders_involved, batting_team) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = tuple(row)
        cursor.execute(query, values)

        row_count += 1
        if row_count % batch_size == 0:
            db_connection.commit()  # Commit the changes in batches

    # Commit the remaining changes
    db_connection.commit()

# Close the cursor and database connection
cursor.close()
db_connection.close()

print("CSV data successfully imported into MySQL table.")
