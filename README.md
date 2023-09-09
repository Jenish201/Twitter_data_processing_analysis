
# Twitter Data Processing and Analysis
In this portfolio project, I tackled the task of processing and analyzing Twitter data. The project involves several key components, including data retrieval from a web server, data storage in a SQLite database, data manipulation and analysis, and performance evaluation.


### Project highlight 
1. **Data Retrieval**: I created a Python function to fetch tweets from a web server using the urllib library. This function allows for specifying the maximum number of tweets to retrieve.

2. **SQLite Database**: I designed a SQLite database structure to store tweets, user information, and geographical data. The database schema includes tables for tweets, users, and geographical information, with appropriate foreign key relationships.

3. **Data Parsing**: I implemented classes to parse and organize the fetched JSON data into meaningful Python objects. These classes include Tweet and User classes for handling tweet and user data, respectively.

4. **Batch Data Insertion**: To optimize data insertion into the database, I implemented batch insertion functions for both tweets and geographical data. This improves efficiency when processing a large number of tweets.

5. **Data Analysis**: I conducted various data analysis tasks, including querying the database to calculate average latitude for each user and measuring the performance of different functions.

6. **Performance Evaluation**: I designed a performance evaluation framework to assess the runtime of key functions under different scenarios, such as data retrieval, data parsing, and data analysis. This helps in understanding the scalability and efficiency of the code.

### Part 1: Data Retrieval and Database Operations
* Part A: Fetching tweets from a web server and populating the database.
* Part B: Loading data into tables directly from the server.
* Part C: Loading data into tables using a locally created file.
* Part D: Batch data insertion for improved efficiency.

### Part 2: Data Analysis and Performance Evaluation
* Part 2A: Initial data analysis and project setup.
* Part 2B: Evaluating the linearity of database query performance.
* Part 2C/D: Measuring the runtime of calculating average latitude using different methods.
* Part 2E/F: Further performance evaluation using regular expressions.

### Part 3: Combining and Exporting Data
* Part 3A: Combining data from multiple tables into a single table for analysis.
* Part 3B: Exporting tweet data to JSON format and comparing file sizes.
* Part 3C: Exporting tweet data to CSV format and comparing file sizes.

### Conclusion:
This project demonstrates my proficency in data processing, database management, and performance analysis. It highlights my ability to work with real-world data and optimize code for efficiency.

### Performance analysis graph
Below is a graph showcasing run times for two different tweet counts, 130000 and 650000 tweets. I ran the same functions for both tweet counts and created a line graph showcasing the different run times for each tweet count.
![A graph showcasing different runtimes for different tweets](https://github.com/Jenish201/Twitter_data_processing_analysis/blob/main/performance_analysis.jpg)
