# Jenish Dobariya
# Due Date: 20th August 2023


# Importing global packages
import hashlib
import json
import sqlite3
import urllib.request
import time
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import re
import os


def reading_web_file(link: str, max_tweet=None):
    """
    Reading tweets from the server and writing it to a file
    :param link: Weblink of where the tweets are stored
    :param max_tweet: Maximum number of tweets to read from the server
    :return: A file containing all the tweets
    """
    data = urllib.request.urlopen(link)
    tweet_count = 0
    with open("Twitter.txt", "w", encoding="utf8") as file:
        for line in data:
            try:
                file.write(line.decode("utf8") + "\n")
                tweet_count += 1
                if tweet_count >= max_tweet:
                    break
            except Exception as e:
                pass


class SqlDataBase:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.create_tables()

    def commit_changes(self):
        self.conn.commit()
        # self.conn.close()

    def create_tables(self):
        cur = self.conn.cursor()
        # Execute a query for creating Tweet table
        cur.execute('''CREATE TABLE IF NOT EXISTS Tweet (
            user_id NUMBER,
            created_at TEXT,
            id_str TEXT PRIMARY KEY,
            text TEXT,
            source TEXT,
            in_reply_to_user_id NUMBER,
            in_reply_to_screen_name TEXT,
            in_reply_to_status_id NUMBER,
            retweet_count NUMBER,
            contributors TEXT,
            geo_id TEXT,
            
            FOREIGN KEY (geo_id) REFERENCES Geo(id)
            FOREIGN KEY (user_id) REFERENCES User(id)
            )''')

        cur.execute("""
        CREATE TABLE IF NOT EXISTS User(
        id NUMBER PRIMARY KEY,
        name TEXT,
        screen_name TEXT,
        description TEXT,
        friends_count NUMBER)""")

        cur.execute('''CREATE TABLE IF NOT EXISTS Geo(
        id TEXT PRIMARY KEY,
        type TEXT,
        longitude NUMBER,
        latitude NUMBER
        )''')

    def insert_tweet(self, tweet):
        cur = self.conn.cursor()
        try:
            # Try to input the data or give error
            cur.execute('INSERT INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                        (tweet.user_id, tweet.created_at, tweet.id_str, tweet.text, tweet.source,
                         tweet.reply_user_id, tweet.reply_to_screen, tweet.reply_to_status,
                         tweet.retweet_count, tweet.contributors, tweet.geo_id))
            geo_info = cur.lastrowid
            return geo_info
        except sqlite3.Error as e:
            # If there is an error for inserting data into the table print the error
            pass
            # print(f'There was an error entering the data to tweet table: {e}')

    def insert_tweet_batch(self, batch):
        cur = self.conn.cursor()
        try:
            cur.executemany("INSERT INTO Tweet VALUES (?,?,?,?,?,?,?,?,?,?,?)", batch)
        except sqlite3.Error:
            pass

    def insert_geo_batch(self, batch):
        cur = self.conn.cursor()
        try:
            cur.executemany("INSERT INTO Geo VALUES (?,?,?,?)", batch)
        except sqlite3.Error as e:
            pass

    def insert_user(self, user_data):
        """
        This function inserts User data into the User table in the database.
        :param user_data: An instance of the User class containing the following attributes:
                    - id (int)
                    - name (str)
                    - screen_name (str)
                    - description (str)
                    - friends_count (int)
        :return: None
        """
        # Establishing a cursor before executing the query
        cur = self.conn.cursor()
        # Execute the query
        try:
            cur.execute('INSERT INTO User VALUES (?,?,?,?,?)',
                        (user_data.id, user_data.name, user_data.screen_name,
                         user_data.description, user_data.friends_count))
        except sqlite3.Error:
            # print(f"Error with inserting this: {e}")
            pass

    def insert_geo(self, tweet):
        """
        Use this function to input the data to Tweet table
        :param tweet: An object of the Tweet class containing the following attributes:
                    - user_id (int)
                    - created_at (str)
                    - id_str (str)
                    - text (str)
                    - source (str)
                    - reply_user_id (int)
                    - reply_to_screen (str)
                    - reply_to_status (str)
                    - retweet_count (int)
                    - contributors (int)
                    - geo_id (str)
                    - geo_type (str)
                    - longitude (int)
                    - latitude (int)
        :return: an error if there was an issue inserting the data
        """
        cur = self.conn.cursor()
        try:
            cur.execute('INSERT INTO Geo VALUES (?,?,?,?)',
                        (tweet.geo_id, tweet.geo_type, tweet.longitude, tweet.latitude))
        except sqlite3.Error as e:
            pass

    def query_table(self, query):
        """
        Executes a query
        :return: returns a tuple containing the result of the  query
        """
        # Establishing a cursor before executing the query
        cur = self.conn.cursor()
        try:
            query_result = cur.execute(query).fetchall()
            return query_result
        except sqlite3.Error as e:
            print(f"There was an error fetching the data due to this: {e}")

    def export_table_to_json(self, table_name, output_file):
        """
        Export the contents of a table to a JSON file
        :param table_name: Name of the table to export
        :param output_file: Output JSON file name
        :return: None
        """
        query = f"SELECT * FROM {table_name}"

        try:
            df = pd.read_sql_query(query, self.conn)
            df.to_json(output_file, orient="records", lines=True)
            print(f"Data from {table_name} exported to {output_file} successfully")
        except sqlite3.Error as e:
            print(f"Error exporting data using pandas: {e}")

    def export_table_to_csv(self, table_name: str, output_file: str):
        """
        Exporting table data to comma separated file
        :param table_name: Name of the table to export
        :param output_file: Output CSV file
        :return: None
        """
        query = f"SELECT * FROM {table_name}"
        try:
            df = pd.read_sql_query(query, self.conn)
            df.to_csv(output_file, index=False, escapechar='\\')
            print(f"Data from {table_name} exported to {output_file} successfully")
        except sqlite3.Error as e:
            print(f"Error exporting query result: {e}")


class Tweet:
    def __init__(self, tdict):
        self.user_id = tdict['user']['id']
        self.created_at = tdict['created_at']
        self.id_str = tdict['id_str']
        self.text = tdict['text']
        self.source = tdict['source']
        self.reply_user_id = tdict['in_reply_to_user_id']
        self.reply_to_screen = tdict['in_reply_to_screen_name']
        self.reply_to_status = tdict['in_reply_to_status_id']
        self.retweet_count = tdict['retweet_count']
        self.contributors = tdict['contributors']

        # Geo information
        geo_info = tdict.get('geo')
        if geo_info:
            self.geo_type = geo_info['type']
            self.longitude, self.latitude = geo_info['coordinates']
            self.geo_id = self.generate_geo_id()
        else:
            self.geo_id = None
            self.geo_type = None
            self.longitude = None
            self.latitude = None

    def generate_geo_id(self):
        """
        Hashing to generate a unique id for geo table
        :return: unique id using longitude and latitude
        """
        # Combining longitude and latitude to create a unique string
        unique_string = f"{self.longitude}{self.latitude}"
        sha256 = hashlib.sha256()
        sha256.update(unique_string.encode('utf-8'))
        return sha256.hexdigest()


class User:
    def __init__(self, user):
        self.id = user['id']
        self.name = user['name']
        self.screen_name = user['screen_name']
        self.description = user['description']
        self.friends_count = user['friends_count']


def reading_data_sql(link: str, db, max_tweets=None):
    """
    Reads tweets from the web link and loads it to the database with appropriate server
    :param link: Link of the webserver
    :param db: Database name
    :param max_tweets: Maximum number of tweets to read.
    :return: A table loaded with data (Returns NONE)
    """
    data = urllib.request.urlopen(link)
    tweet_count = 0
    for line in data:
        try:
            tdict = json.loads(line.decode("utf8"))
            tweet_data = Tweet(tdict)
            user_data = User(tdict['user'])
            if tweet_data.geo_type and tweet_data.longitude and tweet_data.latitude:
                db.insert_geo(tweet_data)
            else:
                pass
            db.insert_user(user_data)
            db.insert_tweet(tweet_data)
            tweet_count += 1
            if tweet_count >= max_tweets:
                break
        except ValueError as e:
            print(f"There was an error inserting this tweet. Error code: {e}")


def reading_data_from_file(file: str, db, max_tweets=None):
    """
    Reading tweets from a file and loading it to a database
    :param file: File name
    :param db: name of the Database
    :param max_tweets: Maximum number of tweets to read in
    :return:
    """
    with open(file, "r", encoding="utf8") as file:
        file.seek(0)
        tweet_count = 0
        for line in file:
            try:
                tdict = json.loads(line.strip())
                tweet_data = Tweet(tdict)
                user_data = User(tdict["user"])
                if tweet_data.geo_type and tweet_data.latitude and tweet_data.longitude:
                    db.insert_geo(tweet_data)
                else:
                    pass
                db.insert_tweet(tweet_data)
                db.insert_user(user_data)
                tweet_count += 1
                if tweet_count >= max_tweets:
                    break
            except ValueError as e:
                pass
        file.close()


def reading_data_from_file_batch(file_path: str, batch_size: int, db, max_tweet=None):
    """
    Read tweets from a file
    :param file_path: Path of the file (str)
    :param batch_size: size of batch you would like to insert in one go (int)
    :param db: database name (str)
    :param max_tweet: maximum number of tweets (int)
    :return: a table filled with data from the file
    """
    with open(file_path, "r", encoding="utf8") as file:
        file.seek(0)
        tweet_batch = []
        geo_batch = []
        tweet_count = 0

        for line in file:
            try:
                tdict = json.loads(line.strip())
                tweet_data = Tweet(tdict)
                user_data = User(tdict['user'])

                if tweet_data.geo_type and tweet_data.latitude and tweet_data.longitude:
                    if tweet_data.geo_id not in geo_batch:
                        geo_batch.append((tweet_data.geo_id, tweet_data.geo_type, tweet_data.longitude,
                                          tweet_data.latitude))

                tweet_batch.append((tweet_data.user_id, tweet_data.created_at, tweet_data.id_str, tweet_data.text,
                                    tweet_data.source,
                                    tweet_data.reply_user_id, tweet_data.reply_to_screen, tweet_data.reply_to_status,
                                    tweet_data.retweet_count, tweet_data.contributors, tweet_data.geo_id))

                if len(geo_batch) >= batch_size:
                    db.insert_geo_batch(geo_batch)
                    geo_batch = []

                if len(tweet_batch) >= batch_size:
                    db.insert_tweet_batch(tweet_batch)
                    tweet_batch = []

                db.insert_user(user_data)
                tweet_count += 1

                if max_tweet is not None and tweet_count >= max_tweet:
                    break
            except ValueError as e:
                pass

        if tweet_batch:
            db.insert_tweet_batch(tweet_batch)

        if geo_batch:
            db.insert_geo_batch(geo_batch)
        file.close()


def query_db_multiple(db):
    query = """SELECT t.user_id, AVG(g.latitude) as avg_latitude,
    (SUM(g.latitude) / COUNT(g.latitude)) as avg_latitude_sum_count
    FROM Tweet AS t INNER JOIN Geo AS g
    ON t.geo_id = g.id
    GROUP BY t.user_id"""
    result = db.query_table(query)
    return result


def linearity_check(num_runs, func, *args):
    total_runtimes = []
    ratio = []
    for num in num_runs:
        total_runtime = 0
        runtimes = []
        for i in range(num):
            start_time = time.time()
            result = func(*args)
            end_time = time.time()
            runtime = end_time - start_time
            total_runtime += runtime
            runtimes.append(runtime)

        total_runtimes.append(total_runtime)
        ratio.append(sum(runtimes) / len(runtimes))

    return ratio


def create_combined_table(db):
    query = """
    CREATE TABLE IF NOT EXISTS Combined AS
    SELECT
        t.*,
        u.name,
        u.screen_name,
        u.description,
        u.friends_count,
        g.type,
        g.longitude,
        g.latitude
    FROM
        Tweet AS t
    LEFT JOIN
        User AS u ON t.user_id = u.id
    LEFT JOIN
        Geo AS g ON t.geo_id = g.id
    """
    cur = db.conn.cursor()
    try:
        cur.execute(query)
        print("Combined table created successfully")
    except sqlite3.Error as e:
        print(f"There was an error creating the combined tabel: {e}")


def calculate_avg_py(file_name: str):
    user_data = []
    with open(file_name, "r", encoding="utf8") as file:
        for line in file:
            try:
                tdict = json.loads(line.strip())
                user_id = tdict['user']['id']
                geo = Tweet(tdict)
                latitude = geo.latitude
                user_data.append((user_id, latitude))

            except ValueError:
                pass
    df = pd.DataFrame(user_data, columns=['user_id', 'latitude'])
    grouped = df.groupby('user_id')['latitude'].agg(['count', 'sum', 'mean'])

    avg_latitude_data = []

    for user_id, data in grouped.iterrows():
        if data['count'] > 0:
            avg_latitude = data['sum'] / data['count']
            mean = data['mean']
            avg_latitude_data.append((user_id, avg_latitude, mean))


def calculate_avg_py_regex(file_name: str):
    user_data = []
    with open(file_name, "r", encoding="utf8") as file:
        for line in file:
            try:
                tdict = json.loads(line.strip())
                user_id_match = re.search(r'"user":\s*\{"id":\s*(\d+)', line)
                latitude_match = re.search(r'"coordinates":\s*\[\s*\d+\.\d+\s*,\s*(-?\d+\.\d+)\s*]', line)
                if user_id_match and latitude_match:
                    user_id = int(user_id_match.group(1))
                    latitude = float(latitude_match.group(1))
                    user_data.append((user_id, latitude))
            except ValueError:
                pass
    df = pd.DataFrame(user_data, columns=['user_id', 'latitude'])
    grouped = df.groupby('user_id')['latitude'].agg(['count', 'sum', 'mean'])

    avg_latitude_data = []

    for user_id, data in grouped.iterrows():
        if data['count'] > 0:
            avg_latitude = data['sum'] / data['count']
            mean = data['mean']
            avg_latitude_data.append((user_id, avg_latitude, mean))


def bytes_to_mb(bytes_size):
    """
    Converting file size from bytes to MB
    :param bytes_size: Byte size (int)
    :return: Size of file in MB
    """
    return bytes_size / (1024 * 1024)


def compare_file_sizes(file_a: str, file_b: str):
    """
    Comparing two file size and printing if the file is smaller or larger than the other.
    :param file_a: Name of the first file
    :param file_b: Name of the second file
    :return: Print statement confirming if file_a is greater/smaller/equal to file_b
    """
    file_a_size = os.path.getsize(file_a)
    file_b_size = os.path.getsize(file_b)
    print(f"Comparing file {file_a} size with {file_b} size")

    print(f"Original File Size: {bytes_to_mb(file_a_size):.2f} bytes")
    print(f"Exported File Size: {bytes_to_mb(file_b_size):.2f} bytes")

    if file_b_size < file_a_size:
        size_diff = bytes_to_mb(file_a_size - file_b_size)
        print(f"The exported file is {size_diff:.2f} MB smaller than the original file.")
    elif file_b_size > file_a_size:
        size_diff = bytes_to_mb(file_b_size - file_a_size)
        print(f"The exported file is {size_diff:.2f} MB larger than the original file.")
    else:
        print("The exported file size is the same as the original file size.")


def part_1(tweet_counts):
    runtimes_per_cnt = []

    for cnt in tweet_counts:
        runtimes = []

        # Part A: Downloading tweets and populating database
        start_time = time.time()
        link = "http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt"
        reading_web_file(link, cnt)
        end_time = time.time()
        runtime_step_1 = end_time - start_time
        runtimes.append(f"{runtime_step_1:.2f}")

        # Part B: Loading the data to tables from the server
        start_time = time.time()
        db = SqlDataBase("twitter.db")
        reading_data_sql(link, db, cnt)
        db.commit_changes()
        end_time = time.time()
        runtime_step_2 = end_time - start_time
        runtimes.append(f"{runtime_step_2:.2f}")

        # Getting row counts for all tables
        row_cnt_tweet = db.query_table("SELECT COUNT(*) FROM Tweet")
        row_cnt_user = db.query_table("SELECT COUNT(*) FROM User")
        row_cnt_geo = db.query_table("SELECT COUNT(*) FROM Geo")

        print(f"Part 1 B row counts for {cnt} tweets \n"
              f"Tweet table row count: {row_cnt_tweet[0][0]} \n"
              f"User table row count: {row_cnt_user[0][0]} \n"
              f"Geo table row count: {row_cnt_geo[0][0]}")

        # Part C: Load the tables using the locally created file
        start_time = time.time()
        db_txt = SqlDataBase("twitter_txt.db")
        file_path = "Twitter.txt"
        reading_data_from_file(file_path, db_txt, cnt)
        db_txt.commit_changes()
        end_time = time.time()
        runtime_step_3 = end_time - start_time
        runtimes.append(f"{runtime_step_3:.2f}")

        # Getting row counts for all tables
        row_cnt_tweet_txt = db_txt.query_table("SELECT COUNT(*) FROM Tweet")
        row_cnt_user_txt = db_txt.query_table("SELECT COUNT(*) FROM User")
        row_cnt_geo_txt = db_txt.query_table("SELECT COUNT(*) FROM Geo")
        print()
        print(f"Part 1 C row counts for {cnt} tweets \n"
              f"Tweet table row count: {row_cnt_tweet_txt[0][0]} \n"
              f"User table row count: {row_cnt_user_txt[0][0]} \n"
              f"Geo table row count: {row_cnt_geo_txt[0][0]}")

        # Part 1 D
        start_time = time.time()
        file_path = "Twitter.txt"
        db_partd = SqlDataBase("partD_database.db")
        batch_size = 2500
        reading_data_from_file_batch(file_path, batch_size, db_partd, cnt)
        db_partd.commit_changes()
        end_time = time.time()
        runtime_step_4 = end_time - start_time
        runtimes.append(f"{runtime_step_4:.2f}")

        # Getting row counts for all tables
        row_cnt_tweet_D = db_partd.query_table("SELECT COUNT(*) FROM Tweet")
        row_cnt_user_D = db_partd.query_table("SELECT COUNT(*) FROM User")
        row_cnt_geo_D = db_partd.query_table("SELECT COUNT(*) FROM Geo")
        print()
        print(f"Part 1 D Row Counts for {cnt} tweets \n"
              f"Tweet table row count: {row_cnt_tweet_D[0][0]} \n"
              f"User table row count: {row_cnt_user_D[0][0]} \n"
              f"Geo table row count: {row_cnt_geo_D[0][0]}")
        print()

        runtimes_per_cnt.append(runtimes)

    print(runtimes_per_cnt)

    step_1_runtimes = [float(r[0]) for r in runtimes_per_cnt]
    step_2_runtimes = [float(r[1]) for r in runtimes_per_cnt]
    step_3_runtimes = [float(r[2]) for r in runtimes_per_cnt]
    step_4_runtimes = [float(r[3]) for r in runtimes_per_cnt]

    # Creating a list of labels for the x-axis
    x_labels = [f"Count: {cnt}" for cnt in tweet_counts]

    # plotting
    plt.figure()
    sns.set_style("whitegrid")
    plt.plot(x_labels, step_1_runtimes, marker='o', label="Part A")
    plt.plot(x_labels, step_2_runtimes, marker='o', label="Part B")
    plt.plot(x_labels, step_3_runtimes, marker='o', label="Part C")
    plt.plot(x_labels, step_4_runtimes, marker='o', label="Part D")

    plt.legend()
    plt.savefig("performance_analysis.jpg")


def part_2():
    num_executes = [5, 20]
    # Part 2 A
    print("Part 2 A completed")
    print()
    db = SqlDataBase("twitter.db")

    # Part 2 B
    db = SqlDataBase("twitter.db")
    query_db_runtimes = linearity_check(num_executes, query_db_multiple, db)
    print("This is for Part 2 B")
    for i, repeat in enumerate(num_executes):
        print(f"Number of executes: {repeat}")
        print(f"Avg Runtimes: {query_db_runtimes[i]:.2f} seconds")

        if i > 0:
            # Comparing the average runtimes between different repetitions
            if query_db_runtimes[i] == query_db_runtimes[i - 1]:
                print("Runtime stays the same")
            else:
                print("Linearity check fails for Part 2 B. Sad :(")
        print()

    # # Part 2 C/D
    tweet_file = "Twitter.txt"
    query_text_runtimes = linearity_check(num_executes, calculate_avg_py, tweet_file)
    print("This is for Part 2 C/D")
    for i, repeat in enumerate(num_executes):
        print(f"Number of executes: {repeat}")
        print(f"Avg Runtimes: {query_text_runtimes[i]:.2f} seconds")

        if i > 0:
            # Comparing the average runtimes between different repetitions
            if query_text_runtimes[i] == query_text_runtimes[i - 1]:
                print("Runtime stays the same")
            else:
                print("Linearity check fails for Part 2 B. Sad :(")
        print()

    # # Part 2 E/F
    tweet_file = "Twitter.txt"
    query_text_runtimes = linearity_check(num_executes, calculate_avg_py_regex, tweet_file)
    print("This is Part 2 E/F")
    for i, repeat in enumerate(num_executes):
        print(f"Number of executes: {repeat}")
        print(f"Avg Runtimes: {query_text_runtimes[i]:.2f} seconds")

        if i > 0:
            # Comparing the average runtimes between different repetitions
            if query_text_runtimes[i] == query_text_runtimes[i - 1]:
                print("Runtime stays the same")
            else:
                print("Linearity check fails for Part 2 B. Sad :(")
        print()


def part_3():
    # Part A
    db = SqlDataBase("twitter.db")
    create_combined_table(db)
    print()

    # Part B
    # Exporting Tweet data
    print("Part 3 B")
    db.export_table_to_json("Tweet", "tweet_data.json")
    compare_file_sizes("Twitter.txt", "tweet_data.json")

    # Exporting Combined data
    print()
    db.export_table_to_json("Combined", "combined_data.json")
    compare_file_sizes("Twitter.txt", "combined_data.json")
    print()

    # Part C
    print("Part 3 C")
    # Exporting Tweet data
    db.export_table_to_csv("Tweet", "tweet_data.csv")
    compare_file_sizes("Twitter.txt", "tweet_data.csv")
    print()
    compare_file_sizes("tweet_data.json", "tweet_data.csv")
    print()

    # Exporting Combined data
    db.export_table_to_csv("Combined", "combined_data.csv")
    compare_file_sizes("Twitter.txt", "combined_data.csv")
    print()
    compare_file_sizes("tweet_data.json", "combined_data.json")


def main():
    # Running Part 1
    tweet_counts = [130000, 650000]
    part_1(tweet_counts)
    print()

    # Running Part 2
    part_2()

    # Running Part 3
    part_3()


if __name__ == '__main__':
    main()
