import os
import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Function to create API Client
def get_aa_client():

    # Load sensitive variables from environment
    api_prod = os.getenv('API_PROD')
    api_dev = os.getenv('API_DEV')
    base_url = api_prod  # You can switch to api_dev for development when needed.
    # base_url = api_dev
    username = os.getenv('API_USERNAME')
    password = os.getenv('API_PASSWORD')
    vid = os.getenv('API_VID')

    api_key = None

    # Create a session to persist HTTP connection across requests
    session = requests.Session()
    session.verify = False  # Disable SSL verification

    # Login function to get the API key
    def login():
        nonlocal api_key
        url = f"{base_url}/login"
        resp = session.post(url, json={
            "UserName": username,
            "Password": password,
            "VID": vid
        })

        if resp.status_code == 200:
            api_key = resp.json()  # Assuming the API key is returned as the response
            print(f"API Key: {api_key}")
        else:
            print(f"Failed to login: {resp.status_code} - {resp.text}")

    # Function to get persons
    def get_persons():
        url = f"{base_url}/persons"
        resp = session.get(url, params={"apiKey": api_key})

        if resp.status_code == 200:
            return resp.json().get("PersonList", [])
        else:
            print(f"Failed to get persons: {resp.status_code} - {resp.text}")
            return []

    # Function to get persons by booking
    def get_persons_by_booking(name):
        url = f"{base_url}/persons/"
        resp = session.post(url, json={
            "Name": name,
        }, params={"apiKey": api_key})

        if resp.status_code == 200:
            return resp.json().get("PersonList", [])
        else:
            print(f"Failed to get persons by booking: {resp.status_code} - {resp.text}")
            return []

    # Function to get credentials
    def get_credentials():
        url = f"{base_url}/credentials"
        resp = session.get(url, params={"apiKey": api_key})

        if resp.status_code == 200:
            return resp.json().get("CredentialList", [])
        else:
            print(f"Failed to get credentials: {resp.status_code} - {resp.text}")
            return []

    # Function to delete a person
    def delete_person(person_id):
        url = f"{base_url}/persons"
        resp = session.delete(url, json={"ID": person_id}, params={"apiKey": api_key})

        if resp.status_code == 200:
            print(f"Person {person_id} deleted successfully")
        else:
            print(f"Failed to delete person: {resp.status_code} - {resp.text}")

    # Function to delete a credential
    def delete_credential(credential_id):
        url = f"{base_url}/credentials"
        resp = session.delete(url, json={"ID": credential_id}, params={"apiKey": api_key})

        if resp.status_code == 200:
            print(f"Credential {credential_id} deleted successfully")
        else:
            print(f"Failed to delete credential: {resp.status_code} - {resp.text}")

    # Call login to initialize the API key
    login()

    # Maintain structure similar to JavaScript functions return
    return {
        "get_persons": get_persons,
        "get_persons_by_booking": get_persons_by_booking,
        "get_credentials": get_credentials,
        "delete_person": delete_person,
        "delete_credential": delete_credential,
    }

# Example usage:
if __name__ == "__main__":
    client = get_aa_client()

    # Fetch persons
    persons = client["get_persons"]()
    print("Persons:", persons)

    # Fetch credentials
    credentials = client["get_credentials"]()
    print("Credentials:", credentials)

    # Fetch persons by booking name
    persons_by_booking = client["get_persons_by_booking"]("John Doe")
    print("Persons by Booking:", persons_by_booking)
