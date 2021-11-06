import asyncpg
import zipfile
import datetime
import asyncio
import shutil
import os
import uvloop

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
                # create the zipfile
                zip = zipfile.ZipFile(
                    f"/{PATH}{str(today)}.zip", "w", zipfile.ZIP_BZIP2
                )
                # connect to the database
                print("Connection to the database")
                connection: asyncpg.Connection = await asyncpg.connect(
                    URL, ssl="require"
                )
                print("Connected to the database")
                # database
                print("Created temp folder")
                os.mkdir("temp")
                for table in TABLES:
                    print(f"fetching up {table} ...")
                    await connection.copy_from_table(
                        table, output=f"temp/{str(today)}-{table}.csv", format="csv"
                    )
                print("closing the connection")
                # close connection
                await connection.close()
                # write to zip
                print("writing to zip file")
                for root, dirs, files in os.walk("temp"):
                    for file in files:
                        zip.write(os.path.join(root, file))
                zip.close()
                print("deleting temp file")
                shutil.rmtree("temp")
                print(f"{str(today)} backup: done")
            # sleep until so the script is not reasource intensive
            await asyncio.sleep(60 * 60)
        except Exception as exc:
            print(exc)


if __name__ == "__main__":
    print("started script")
    uvloop.install()
    asyncio.run(backup_loop())