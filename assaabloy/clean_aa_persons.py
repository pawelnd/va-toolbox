import asyncio
import os
import time
from dotenv import load_dotenv
import aiohttp
import logging

from assaabloy.client import get_aa_client
from assaabloy.db_client import get_db_client

# Load environment variables from .env
load_dotenv()

# Constants
TO_DELETE_STATUS = ['TO_BE_CANCELLED', 'CLOSED', 'TO_BE_CLOSED']

# Error handlers
error_count = 0
deleted_count = 0

# Sleep function for retrying (no direct equivalent to Javascript's promise)
async def sleep(ms):
    await asyncio.sleep(ms / 1000)

# Main function to get client, process persons
async def main():
    global error_count, deleted_count

    # Placeholder for importing clients based on your tools.
    db = await get_db_client()
    client = await get_aa_client()  # Assuming an async method exists for this
   # Assuming another async method exists for this

    start = time.time()  # Track time execution

    # Fetch persons
    persons = await client.get_persons()
    personsb = await client.get_persons_by_booking("661463")
    persons_ids = [p['ID'] for p in persons]

    # Fetch credentials and filter
    credentials = await client.get_credentials()

    exist, notexist = 0, 0
    for credential in credentials:
        if credential['PrsId'] not in persons_ids:
            notexist += 1
            await client.delete_credential(credential['ID'])  # Assume async call to delete
        else:
            exist += 1

    logging.info(f"Exist: {exist}, Not Exist: {notexist}")

    stop = time.time() - start  # Execution time
    logging.info(f"Execution Time: {stop} seconds")
    logging.info(f"Person Count: {len(persons)}")

    # Process persons to check their booking info and delete if TO_DELETE_STATUS matches
    for person in persons:
        if error_count > 10:
            logging.error("Error limit exceeded")
            return

        person_id = person['ID']
        person_name = person['Name']

        # Split name using underscore
        parts = person_name.split('_')
        if len(parts) <= 2:
            continue

        booking_id = parts[-1]

        if booking_id.isdigit():
            booking = await db.get_booking_by_id(booking_id)  # Assume async DB lookup

            if not booking:
                continue

            # Fetch booking details
            status = booking.get('status')
            booking_no = booking.get('booking_no')
            date_to = booking.get('date_to')

            logging.info(f"Processing: {booking_no}, Status: {status}, To Date: {date_to}, Errors: {error_count}, Deleted: {deleted_count}")

            if status in TO_DELETE_STATUS:
                try:
                    await client.delete_person(person_id)  # Attempt person deletion
                    deleted_count += 1
                except Exception as e:
                    error_count += 1
                    await sleep(5000)
                    logging.error(f"Failed to delete person {person_name}, ID: {person_id}, Retrying...")
                    logging.error(e)

                    # Retry deletion after re-login
                    try:
                        await client.login()  # Assuming re-login is needed
                        await client.delete_person(person_id)  # Retry deletion
                        deleted_count += 1
                        error_count = 0  # Reset error count after success
                    except Exception as e2:
                        logging.error(f"Failed to delete person twice {person_name}, ID: {person_id}")
                        logging.error(e2)
                        continue
        else:
            logging.info(f"Skipping person due to non-numeric booking ID: {person_name}")

    logging.info(f"Processed {len(persons)} persons.")

# Async client import placeholde


# Run the script using asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
