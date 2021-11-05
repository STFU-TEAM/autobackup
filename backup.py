import asyncpg
import gzip
import datetime
import asyncio
import os

URL = os.environ["DATABASE_URL"]
PATH = os.environ["SAVEPATH"]
TABLES = ["global", "ban", "gang", "servers", "users", "webhook"]


async def backup_loop():
    print("Started backup loop...")
    today = datetime.datetime.date(datetime.datetime.min)
    while True:
        try:
            # check if the backup has been run today
            if today != datetime.datetime.date(datetime.datetime.now()):
                today = datetime.datetime.date(datetime.datetime.now())
                # connect to the database
                connection: asyncpg.Connection = await asyncpg.connect(
                    URL, ssl="require"
                )
                # database
                for table in TABLES:
                    result = await connection.copy_from_table(
                        table, output=f"{str(today)}-{table}.csv", format="csv"
                    )
                    # compress backup gzip file
                    with gzip.open(f"{PATH}{str(today)}-{table}.csv.gz", "wb") as f:
                        f.write(result)
                # close connection
                await connection.close()
                print(f"{str(today)} backup: done")
            await asyncio.sleep(60 * 60)
        except Exception as exc:
            print(exc)


if __name__ == "__main__":
    print("started script")
    asyncio.run(backup_loop())