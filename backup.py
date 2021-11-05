import asyncpg
import gzip
import datetime
import asyncio
import os
import uvloop
import csv
import pandas as pd

URL = os.environ["DATABASE_URL"]
PATH = os.environ["SAVEPATH"]
TABLES = ["global", "ban", "gang", "servers", "users", "webhook"]


async def backup_loop():
    print("Started backup loop...")
    today = datetime.datetime.date(datetime.datetime.min)
    while True:
        try:
            print(
                "Date check:", today != datetime.datetime.date(datetime.datetime.now())
            )
            # check if the backup has been run today
            if today != datetime.datetime.date(datetime.datetime.now()):
                today = datetime.datetime.date(datetime.datetime.now())
                # connect to the database
                print("Connection to the database")
                connection: asyncpg.Connection = await asyncpg.connect(
                    URL, ssl="require"
                )
                print("Connected to the database")
                # database
                for table in TABLES:
                    print(f"Backing up {table} ...")
                    result: str = await connection.copy_from_table(
                        table, output=f"{str(today)}-{table}.csv", format="csv"
                    )
                    # convert into a csv
                    csv_file = csv.reader(result.splitlines())
                    dataframe = pd.DataFrame(csv_file)
                    # compress backup gzip file
                    dataframe.to_csv(
                        f"/{PATH}{str(today)}-{table}.csv.zip", compression="zip"
                    )
                    print(f"Backed up {table}")
                print("closing the connection")
                # close connection
                await connection.close()
                print(f"{str(today)} backup: done")
            await asyncio.sleep(60 * 60)
        except Exception as exc:
            print(exc)


if __name__ == "__main__":
    print("started script")
    uvloop.install()
    asyncio.run(backup_loop())