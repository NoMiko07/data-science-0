import os
import glob
import psycopg2
from dotenv import load_dotenv

def get_env() -> dict:
    """ get env variables and store it into a dict then return it """

    load_dotenv("../.env")
    env_dict = {}
    env_dict["DB_NAME"] = os.getenv("POSTGRES_DB")
    env_dict["DB_USER"] = os.getenv("POSTGRES_USER")
    env_dict["DB_PASSWORD"] = os.getenv("POSTGRES_PASSWORD")
    env_dict["DB_HOST"] = os.getenv("POSTGRES_HOST")
    env_dict["DB_PORT"] = os.getenv("POSTGRES_PORT")
    iterator = iter(env_dict)

    while True:
        try:
            key = next(iterator) 
            if env_dict[key] == None:
                print("One or few keys are missing")
                exit()
        except StopIteration:
            break  

    return env_dict

def trim_folder_and_extension(path: str) -> str:
    path = path.split("/")[-1]
    return path.replace(".csv", "")

def csv_checker(path: list, filenames_with_path: list):
    if path is not None and not any(item in filenames_with_path for item in path):
        print(f"The path is incorrect -> {path}")
        exit()
    elif path == None and not filenames_with_path:
        print("NO CSV files found.")
        exit()    
    print("CSV checked!✅")
    
def get_filenames_with_path(path: list) -> list:
    filenames_with_path = glob.glob('../customer/*.csv')
    filenames_with_path = [f.replace("\\", "/") for f in filenames_with_path]
    csv_checker(path, filenames_with_path)

    return filenames_with_path


def connect_to_db(env_dict: dict, path: list = None):
    try:
        filenames_with_path = get_filenames_with_path(path)
        if path is not None:
            filenames_with_path = path
        for file in filenames_with_path:
            table_name = trim_folder_and_extension(file)
            conn = psycopg2.connect(
                dbname=env_dict["DB_NAME"],
                user=env_dict["DB_USER"],
                password=env_dict["DB_PASSWORD"],
                host=env_dict["DB_HOST"],
                port=env_dict["DB_PORT"]
            )
            cur = conn.cursor()
            print(f"Creating '{table_name}' table ...")
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                event_time TIMESTAMPTZ,
                event_type VARCHAR(255),
                product_id INT,
                price DECIMAL(10, 2),
                user_id BIGINT,
                user_session UUID
            );
            """
            cur.execute(create_table_query)
            print(f"Successfully created table '{table_name}'! ✅")
            print(f"Transferring data to table'{table_name}'...")
            with open(file, 'r') as f:
                cur.copy_expert(f"COPY {table_name}(event_time, event_type, product_id, price, user_id, user_session) FROM STDIN WITH CSV HEADER", f)
            conn.commit()
            print(f"Data transferred successfully to table '{table_name}'! ✅")

    except Exception as e:
        print(f"❌ Erreur lors de l'insertion des données : {e}")

    finally:
        # Close the connection
        if cur:
            cur.close()
        if conn:
            conn.close()


            
def main():
    env_dict = get_env()
    path = ["../customer/data_2022_oct.csv"]
    connect_to_db(env_dict, path)

if __name__ == "__main__":
    main()