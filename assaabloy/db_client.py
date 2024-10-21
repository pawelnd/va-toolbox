import os
import asyncpg
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

async def get_db_client():
    """
    This function creates an asynchronous connection to the PostgreSQL database 
    and returns a client object with a method to query bookings by ID.
    """

    # Fetch database credentials from environment variables
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')  # PostgreSQL default port
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    # Create a connection pool for PostgreSQL using asyncpg
    pool = await asyncpg.create_pool(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name,
        ssl=False  # Set SSL to meet specific requirements (adjust if needed)
    )

    # Define the get_booking_by_id as an inner function
    async def get_booking_by_id(booking_id):
        """Retrieve a booking by its ID."""
        async with pool.acquire() as conn:  # Acquire the connection
            sql = """
            SELECT * FROM reservation.booking
            WHERE id = $1
            LIMIT 1
            """
            row = await conn.fetchrow(sql, booking_id)  # Fetch a single row
            return row

    # Return client functions
    return {
        'get_booking_by_id': get_booking_by_id,
        'pool': pool  # Exposes the connection pool if needed for other queries
    }

# Example usage inside an async function
async def example_query():
    # Get the database client
    db = await get_db_client()

    # Example of how to fetch a booking by its ID
    booking_id = 661463
    booking = await db['get_booking_by_id'](booking_id)
    print('Booking:', booking)

# Run the example
if __name__ == "__main__":
    import asyncio
    asyncio.run(example_query())
