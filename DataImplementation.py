from DataInterface import DataInterface
from google.cloud import bigquery
from Client import Client
from Client import check_type
from datetime import datetime


class DataImplementation(DataInterface):
    """
    Perform operations for client into a specified Big Query table
    project -> name of project in GCP (String)
    dataset -> name of dataset in GCP (String)
    table_name -> name of the table to be updated in BQ (String)
    service_account_json -> json structure with GCP tokens

    """
    def __init__(self,table_name, service_account_json,file_name,project,dataset, client):
        self.table_name = table_name
        self.project = project
        self.dataset = dataset
        self.file = file_name
        self.table_id = f'{self.project}.{self.dataset}.{table_name}'
        # create big query client
        self.client = bigquery.Client.from_service_account_json(service_account_json)

    def add_elements(self, data):
        """
        :param data: Client object with data to be updated
        :return:
        """
        data = check_type(data, Client)
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        query = (f"insert `{self.table_id}` (name, middle_name, last_name, age, timestamp)\
                                             values ('{data.client.name}',\
                                                    '{data.client.middle_name}',\
                                                    '{data.client.last_name}',\
                                                    '{data.client.age},\
                                                     {dt_string})")
        # update Big Query Table
        query_job = self.client.query(query)
        print(query_job.result)

    def get_elements(self):
        """
        Prints all elements in the table and returns a list of Client objects
        """
        query = f"select name, middle_name, last_name, age from `{self.table_id}`"
        query_job = self.client.query(query)
        clients = []
        for row in query_job:
            print('Name', row['name'], 'middle name:', row['middle_name'], 'last name: ',row['last_name'], 'age:', row['age'])
            clients.append(Client(row['name'],row['middle_name'],row['last_name'],row['age']))
        return clients


    def average(self,start_window, end_window):
        """get moving avg of client ages in a window of time"""
        query = f"select avg(age) from `{self.table_id}` where timestamp between {start_window} and {end_window}"
        query_job = self.client.query(query)
        return query_job.result
