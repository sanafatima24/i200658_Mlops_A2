# from datetime import timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.utils.dates import days_ago
# import pandas as pd
# import requests
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# import re
# import logging

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
# }

# def extract_links(url):
#     try:
#         response = requests.get(url)
#         response.raise_for_status()  # Raise an exception for HTTP errors
#         soup = BeautifulSoup(response.content, 'html.parser')
#         base_url = response.url
#         links = [urljoin(base_url, link.get('href')) for link in soup.find_all('a', href=True)]
#         return links
#     except requests.RequestException as e:
#         print(f"Error occurred while accessing {url}: {e}")
#         return []

# def extract_title_and_description(link):
#     try:
#         response = requests.get(link)
#         response.raise_for_status()
#         soup = BeautifulSoup(response.content, 'html.parser')
#         title = soup.title.get_text() if soup.title else ""
#         description_tag = soup.find('meta', {'name': 'description'})
#         description = description_tag['content'] if description_tag else ""
#         return title, description
#     except requests.RequestException as e:
#         print(f"Error occurred while accessing {link}: {e}")
#         return "", ""
#     except KeyError:
#         return "", ""

# def extract_data():
#     dawn_links = extract_links('https://www.dawn.com/')
#     bbc_links = extract_links('https://www.bbc.com/')
#     data = []
#     for link in dawn_links + bbc_links:
#         title, description = extract_title_and_description(link)
#         data.append({'link': link, 'title': title, 'description': description})
#     df = pd.DataFrame(data)
#     df.to_csv('dataset_news.csv', index=False)

# def transform_data():
#     news_links_dataset_v1 = pd.read_csv('dataset_news.csv')
#     news_links_dataset_v2 = news_links_dataset_v1.drop_duplicates()
#     news_links_dataset_v3 = news_links_dataset_v2.dropna()
#     def clean_text(text):
#         text = text.lower()
#         text = re.sub(r'[^a-zA-Z\s]', '', text)
#         return text
#     news_links_dataset_v3['cleaned_description'] = news_links_dataset_v3['description'].apply(clean_text)
#     news_links_dataset_v3.to_csv('dataset_news_new.csv', index=False)

# dag = DAG(
#     'Data_Extraction_Transformation_Loading_3',
#     default_args=default_args,
#     description='DAG to automate Data Extraction, Data Transformation, and Data Loading',
#     schedule_interval='@daily',
# )

# # BashOperator to initialize Git repository
# git_init_command = "git init"
# git_init_task = BashOperator(
#     task_id='git_init',
#     bash_command=git_init_command,
#     dag=dag,
# )


# # BashOperator to initialize DVC
# dvc_init_command = "dvc init --force --no-scm"
# dvc_init_task = BashOperator(
#     task_id='dvc_init',
#     bash_command=dvc_init_command,
#     dag=dag,
# )

# extract_data_task = PythonOperator(
#     task_id='extract_data',
#     python_callable=extract_data,
#     dag=dag,
# )

# transform_data_task = PythonOperator(
#     task_id='transform_data',
#     python_callable=transform_data,
#     dag=dag,
# )


# # Add BashOperator to add Google Drive remote
# dvc_remote_add_command = "dvc remote add -d my_google_drive gdrive://1i3kGKsxTKMw3-tN9wpJ6fHjrIOoFaO0j -f"
# dvc_remote_add_task = BashOperator(
#     task_id='dvc_remote_add',
#     bash_command=dvc_remote_add_command,
#     dag=dag,
# )

# # Add BashOperator to add data to DVC
# dvc_add_command = "dvc add dataset_news_new.csv"  # Assuming the CSV is in the same directory as where the DAG runs
# dvc_add_task = BashOperator(
#     task_id='dvc_add',
#     bash_command=dvc_add_command,
#     dag=dag,
# )

# # Add BashOperator to push data with DVC
# dvc_push_command = "dvc push"
# dvc_push_task = BashOperator(
#     task_id='dvc_push',
#     bash_command=dvc_push_command,
#     dag=dag,
# )

# # Define the task dependencies
# git_init_task >> dvc_init_task
# dvc_init_task >> dvc_remote_add_task
# dvc_remote_add_task >> extract_data_task >> transform_data_task >> dvc_add_task >> dvc_push_task



from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        soup = BeautifulSoup(response.content, 'html.parser')
        base_url = response.url
        links = [urljoin(base_url, link.get('href')) for link in soup.find_all('a', href=True)]
        return links
    except requests.RequestException as e:
        print(f"Error occurred while accessing {url}: {e}")
        return []

def extract_title_and_description(link):
    try:
        response = requests.get(link)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        title = soup.title.get_text() if soup.title else ""
        description_tag = soup.find('meta', {'name': 'description'})
        description = description_tag['content'] if description_tag else ""
        return title, description
    except requests.RequestException as e:
        print(f"Error occurred while accessing {link}: {e}")
        return "", ""
    except KeyError:
        return "", ""

def extract_data():
    dawn_links = extract_links('https://www.dawn.com/')
    bbc_links = extract_links('https://www.bbc.com/')
    data = []
    for link in dawn_links + bbc_links:
        title, description = extract_title_and_description(link)
        data.append({'link': link, 'title': title, 'description': description})
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/dvc_repo/dataset_news.csv', index=False)

def transform_data():
    news_links_dataset_v1 = pd.read_csv('/opt/airflow/dvc_repo/dataset_news.csv')
    news_links_dataset_v2 = news_links_dataset_v1.drop_duplicates()
    news_links_dataset_v3 = news_links_dataset_v2.dropna()
    def clean_text(text):
        text = text.lower()
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        return text
    news_links_dataset_v3['cleaned_description'] = news_links_dataset_v3['description'].apply(clean_text)
    news_links_dataset_v3.to_csv('/opt/airflow/dvc_repo/dataset_news_new.csv', index=False)

dag = DAG(
    'Data_Extraction_Transformation_Loading',
    default_args=default_args,
    description='DAG to automate Data Extraction, Data Transformation, and Data Loading',
    schedule_interval='@daily',
)

install_dvc_gdrive = BashOperator(
    task_id='install_dvc_gdrive',
    bash_command='pip install dvc[gdrive]',
    dag=dag,
)

check_dvc_gdrive = BashOperator(
    task_id='check_dvc_gdrive',
    bash_command='python -c "import dvc_gdrive"',
    dag=dag,
)


# Directory creation for DVC operations
create_dvc_directory = BashOperator(
    task_id='create_dvc_directory',
    bash_command='mkdir -p /opt/airflow/dvc_repo',
    dag=dag,
)

# BashOperator to initialize DVC in the specified directory
dvc_init_task = BashOperator(
    task_id='dvc_init',
    bash_command='cd /opt/airflow/dvc_repo && dvc init --force --no-scm',
    dag=dag,
)

# Other tasks...
dvc_remote_add_task = BashOperator(
    task_id='dvc_remote_add',
    bash_command='cd /opt/airflow/dvc_repo && dvc remote add -d my_google_drive gdrive://1i3kGKsxTKMw3-tN9wpJ6fHjrIOoFaO0j -f',
    dag=dag,
)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

dvc_add_command = f"cd /opt/airflow/dvc_repo && pwd && dvc add dataset_news_new.csv"
dvc_add_task = BashOperator(
    task_id='dvc_add',
    bash_command=dvc_add_command,
    dag=dag,
)

# Add BashOperator to push data with DVC
dvc_push_command = "cd /opt/airflow/dvc_repo && pwd && dvc push"
dvc_push_task = BashOperator(
    task_id='dvc_push',
    bash_command=dvc_push_command,
    dag=dag,
)

# Define task dependencies
create_dvc_directory >> install_dvc_gdrive >> check_dvc_gdrive >> dvc_init_task
dvc_init_task >> dvc_remote_add_task  
dvc_remote_add_task >> extract_data_task >> transform_data_task >> dvc_add_task >> dvc_push_task