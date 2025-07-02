#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "requests",
#   "python-dotenv",
# ]
# ///
"""
Fivetran API - Create a Fivetran User

Requirements:
- pip install uv
- chmod +x <this file>
- Create a .env file with FIVETRAN_API_KEY and FIVETRAN_API_SECRET
"""

import requests
import os
from base64 import b64encode
from dotenv import load_dotenv

class FivetranUserManager:
    """
    A class to handle Fivetran user management operations.
    """

    def __init__(self):
        """Initialize the Fivetran API client with credentials."""
        load_dotenv()

        self.base_url = "https://api.fivetran.com"
        self.api_key = os.getenv('FIVETRAN_API_KEY')
        self.api_secret = os.getenv('FIVETRAN_API_SECRET')

        if not self.api_key or not self.api_secret:
            raise ValueError("Please set FIVETRAN_API_KEY and FIVETRAN_API_SECRET in your .env file")

        # Create basic auth header
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded_credentials = b64encode(credentials.encode()).decode()

        self.headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Accept": "application/json"
        }

    def _make_request(self, method, endpoint, data=None):
        """
        Make an authenticated request to the Fivetran API.

        Args:
            method (str): HTTP method (GET, POST, etc.)
            endpoint (str): API endpoint
            data (dict, optional): Request data for POST requests

        Returns:
            dict: API response data or None if failed
        """
        url = f"{self.base_url}{endpoint}"
        headers = self.headers.copy()

        if data:
            headers["Content-Type"] = "application/json"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                timeout=30
            )

            return self._handle_response(response)

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {str(e)}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            return None

    def _handle_response(self, response):
        """
        Handle API response and print appropriate messages.

        Args:
            response: requests Response object

        Returns:
            dict: Response data or None if failed
        """
        if response.status_code in [200, 201]:
            return response.json()

        # Handle common error responses
        error_messages = {
            400: "❌ Bad Request - Invalid data",
            401: "❌ Unauthorized - Check your API credentials",
            403: "❌ Forbidden - Insufficient permissions",
            409: "❌ Conflict - Resource already exists (user email may already exist)"
        }

        if response.status_code in error_messages:
            print(error_messages[response.status_code])
            try:
                error_data = response.json()
                if 'message' in error_data:
                    print(f"Details: {error_data['message']}")
            except:
                pass
        else:
            print(f"❌ Request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")

        return None

    def get_users(self):
        """
        Retrieves a list of all users in the Fivetran account.

        Returns:
            list: List of user dictionaries or None if failed
        """
        print("Fetching users from Fivetran...")
        print("-" * 50)

        result = self._make_request("GET", "/v1/users")

        if result:
            data = result.get('data', {})
            users = data.get('items', [])
            print(f"✅ Found {len(users)} users:")
            print("-" * 50)

            for user in users:
                self._print_user_details(user)

            return users

        return None

    def create_user(self, email, given_name, family_name, phone, role="Account Administrator"):
        """
        Creates a new user in Fivetran.

        Args:
            email (str): User's email address
            given_name (str): User's first name
            family_name (str): User's last name
            phone (str): User's phone number
            role (str): User's role (default: Account Administrator)

        Returns:
            dict: Created user data or None if failed
        """
        user_data = {
            "email": email,
            "family_name": family_name,
            "given_name": given_name,
            "phone": phone,
            "role": role
        }

        print(f"Creating user: {given_name} {family_name}")
        print(f"Email: {email}")
        print(f"Role: {role}")
        print("-" * 50)

        result = self._make_request("POST", "/v1/users", user_data)

        if result:
            user = result.get('data', {})
            print("✅ User created successfully!")
            print(f"User ID: {user.get('id')}")
            print(f"Status: {'Invited' if user.get('invited') else 'Active'}")
            print(f"Email Verified: {user.get('verified')}")
            print("\n📧 An invitation email has been sent to the user.")
            print("The user will need to accept the invitation to complete setup.")

            return result

        return None

    def _print_user_details(self, user):
        """
        Print formatted user details.

        Args:
            user (dict): User data dictionary
        """
        print(f"Name: {user.get('given_name', '')} {user.get('family_name', '')}")
        print(f"Email: {user.get('email', 'N/A')}")
        print(f"Role: {user.get('role', 'N/A')}")
        print(f"Status: {'Active' if user.get('active', False) else 'Inactive'}")
        print(f"Verified: {'Yes' if user.get('verified', False) else 'No'}")
        print(f"User ID: {user.get('id', 'N/A')}")
        print(f"Last Login: {user.get('logged_in_at', 'Never')}")
        print("-" * 30)


def main():
    """
    Main function with menu to choose between listing users or creating a user.
    """
    print("Fivetran User Management Script")
    print("=" * 50)

    try:
        manager = FivetranUserManager()
    except Exception as e:
        print(f"❌ Failed to initialize: {str(e)}")
        return

    while True:
        print("\nChoose an action:")
        print("1. List all users")
        print("2. Create new user")
        print("3. Exit")

        choice = input("\nEnter your choice (1-3): ").strip()

        if choice == '1':
            manager.get_users()

        elif choice == '2':
            # Default user data - you can modify this or make it interactive
            manager.create_user(
                email="noel@example.com",
                given_name="Noel",
                family_name="Gomez",
                phone="+2125551212",
                role="Account Administrator"
            )

        elif choice == '3':
            print("Goodbye!")
            break

        else:
            print("❌ Invalid choice. Please select 1, 2, or 3.")


if __name__ == "__main__":
    main()
