import asyncio
import os

import asyncpg
from dotenv import load_dotenv
load_dotenv()
# Define database connection parameters
conn_params = {
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

# Define the queries
SELECT_BOOKINGS_QUERY = """
SELECT booking.id
FROM reservation.booking
JOIN reservation.object_in_building_booking 
    ON booking.id = object_in_building_booking.booking_id
JOIN building.object_in_building 
    ON object_in_building_booking.object_in_building_id = object_in_building.id
JOIN building.building 
    ON object_in_building.building_id = building.id
WHERE building.code = 'DE-10178-A'
  AND booking.date_to >= NOW() -- Booking ends after now
  AND booking.date_from <= NOW() + INTERVAL '7 days' -- Booking starts by next week
  AND booking.is_deleted = false;
"""

SOFT_DELETE_QUERY = """
UPDATE reservation.booking 
SET is_deleted = TRUE 
WHERE id = $1;
"""

async def get_bookings_to_delete(pool):
    """ Fetch all the bookings that are due for soft deletion. Returns a list of bookings """
    async with pool.acquire() as conn:
        return await conn.fetch(SELECT_BOOKINGS_QUERY)  # Fetch rows as list of records (dict-like)

async def soft_delete_booking(booking_id, pool, semaphore):
    """ Soft delete a booking given the booking ID, within a semaphore lock to limit concurrency """
    async with semaphore:  # This will block when more than 5 tasks are active
        try:
            async with pool.acquire() as conn:  # Acquiring a connection for this task
                await conn.execute(SOFT_DELETE_QUERY, booking_id)
                print(f"Soft deleted booking with id: {booking_id}")
        except Exception as e:
            print(f"Failed to soft delete booking with id={booking_id}: {e}")

async def main():
    conn_pool = None
    try:
        # Step 1: Create a database connection pool
        conn_pool = await asyncpg.create_pool(**conn_params)

        # Step 2: Fetch all bookings that need to be soft deleted
        bookings = await get_bookings_to_delete(conn_pool)
        if not bookings:
            print("No bookings found for soft deletion.")
            return

        print(f"Found {len(bookings)} bookings for soft deletion.")

        # Step 3: Create a semaphore to limit concurrency to 5 tasks
        semaphore = asyncio.Semaphore(5)

        # Step 4: Create tasks where each task soft deletes a booking within the semaphore
        delete_tasks = [soft_delete_booking(booking['id'], conn_pool, semaphore) for booking in bookings]

        # Use asyncio.gather to handle tasks concurrently with a concurrency limit
        await asyncio.gather(*delete_tasks)

        print("All applicable bookings have been successfully soft deleted.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection pool only if it was created
        if conn_pool:
            await conn_pool.close()

if __name__ == "__main__":
    # Use asyncio.run to execute the main coroutine
    asyncio.run(main())
