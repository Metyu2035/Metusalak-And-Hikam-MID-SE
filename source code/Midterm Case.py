import mysql.connector as mysql
from mysql.connector import Error
import sqlalchemy
import os
import pandas as pd

class StudiKasus2:
    """
    StudiKasus2 as class 
    have variable there are host, port and user
    this will connect us from python to database.
    "mysql.connect" is function, with this we can connected to db
    host, port, user, and password parameter from def _init_
    """
    def _init_(self, host, port, user, password):
        self.host = 'localhost' #our host name
        self.port = '3306' #our port number
        self.user = 'root' #our user name
        self.password = os.environ['PW_DB'] #with this, people cant see our db password

        self.conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )

    def connect_db(self):
        """
        #connect to Database
        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                print("Database connected!")
                # If the connection is successfull
        except Error as e:           
            print("Error while connecting to MySQL", e)
    
    def create_db(self, db_name):
        """
        this is for create database,
        db_name is the parameter 
        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute("CREATE DATABASE {}".format(db_name))
                print('Database ', db_name, ' created!')
        except Error as e:
            print("Error while connecting to MySQL", e)

    def import_csv(self, path):
        """
        Fill the path attribute with your csv file.
        """
        global df
        df = pd.read_csv(path, index_col=False, delimiter=',', encoding='latin1')
    
    def imp_df():
        """
        Function to get the data file (df) from import_csv() function.
        """
        return df

    def create_table(self, db_name, table_name, df):
        """
        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute("USE {}".format(db_name))
                cursor.execute("CREATE TABLE {}".format(table_name))
        except Error as e:
            print("Error while connecting to MySQL", e)

        engine_stmt = 'mysql+mysqlconnector://%s:%s@%s:%s/%s' % (self.user, self.password,
                                                            self.host, self.port, db_name)
        engine = sqlalchemy.create_engine(engine_stmt)
                
        df.to_sql(name=table_name, con=engine,
                if_exists='append', index=False, chunksize=1000)

    
    def load_data(self, db_name, table_name):
        """

        """
        try:
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM {}.{}".format(db_name, table_name))
                result = cursor.fetchall()
                return result
        except Error as e:
            print("Error while connecting to MySQL", e)