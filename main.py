import pymysql
import pandas as pd
import time
from datetime import datetime
from csv import writer


def append_to_csv(file_name, list_of_elem):
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)


now = datetime.now()
start_time = time.time()
exec_time = now.strftime("%d/%m/%Y %H:%M:%S")

# Connection Configuration

endpoint = "XX"
username = "XX"
password = "AXX"
database_name = "employees"

# Connection
connection = pymysql.connect(host=endpoint, user=username, passwd=password, db=database_name)

# Query and append to panda database
try:
    with connection.cursor() as cursor:
        query = "SELECT * FROM salaries LEFT OUTER JOIN employees on salaries.emp_no=employees.emp_no " \
                "LEFT OUTER JOIN dept_emp on salaries.emp_no=dept_emp.emp_no " \
                "LEFT OUTER JOIN departments on dept_emp.dept_no=departments.dept_no " \
                "LEFT OUTER JOIN dept_manager on salaries.emp_no=dept_manager.emp_no " \
                "LEFT OUTER JOIN titles on salaries.emp_no=titles.emp_no;"
        chunks = []
        for chunk in pd.read_sql(query, connection, chunksize=1000):
            chunks.append(chunk)
        # print(len(chunks))
        result = pd.concat(chunks, ignore_index=True)
        # print(type(result))
        print(result)
finally:
    connection.close()


latency = (time.time() - start_time)
result = [exec_time, latency]
append_to_csv('AWS_research_data.csv', result)
