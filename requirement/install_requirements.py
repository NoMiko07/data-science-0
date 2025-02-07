import subprocess
import sys

def install_requirements():
    try:
        # Check if the requirements.txt file exists
        with open('requirements.txt', 'r') as file:
            print("requirements.txt found, installing dependencies...")

        # Install the dependencies using pip
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

        print("Dependencies installed successfully! ✅")

    except FileNotFoundError:
        print("'requirements.txt' file not found. ❌")
    except subprocess.CalledProcessError as e:
        print(f"Error during installation: {e} ❌")
    except Exception as e:
        print(f"An unexpected error occurred: {e} ❌")

if __name__ == "__main__":
    install_requirements()
