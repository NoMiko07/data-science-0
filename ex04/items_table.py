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

def csv_checker(path: str):
    filename_with_path = glob.glob('../item/*.csv')
    filename_with_path = [f.replace("\\", "/") for f in filename_with_path]
    print(filename_with_path)
    if path not in filename_with_path:
        print(f"The path is incorrect -> {path}")
        exit()
    elif not filename_with_path:
        print("NO CSV files found.")
        exit()    
    print("CSV checked!✅")

def connect_to_db(env_dict: dict):
    try:
        file_path = "../item/item.csv"
        table_name = "items"
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
            product_id INT,
            category_id BIGINT,
            category_code TEXT,
            brand VARCHAR(50)
        );
        """
        cur.execute(create_table_query)
        print(f"Successfully created table '{table_name}'! ✅")
        print(f"Transferring data to table'{table_name}'...")
        with open(file_path, 'r') as f:
            cur.copy_expert(f"COPY {table_name}(product_id, category_id, category_code,brand) FROM STDIN WITH CSV HEADER", f)
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
    csv_checker("../item/item.csv")
    connect_to_db(env_dict)

if __name__ == "__main__":
    main()