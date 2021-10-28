import os
import shutil
import datetime
import asyncio
from zipfile import ZipFile
import argparse

from loguru import logger


class ArchiveService:
    def __init__(self, storage_path, archive_path):
        self.storage_path = storage_path
        self.archive_path = archive_path

    @staticmethod
    def check_no_disk_space_left():
        total, _, free = shutil.disk_usage("/")
        if free / total < 0.1:
            return True
        return False

    def compress_oldest_file(self):
        all_files = []
        for directory, sub_dirs, files in os.walk(self.storage_path):
            all_files += files
        if len(all_files):
            oldest_file = min(all_files)
            file_name, file_extension = os.path.splitext(oldest_file)
            _, year, month, day, file_name = file_name.split('/')
            logger.info(f"compress {oldest_file}")
            archive = ZipFile(f'{self.archive_path}/{year}/{month}/{day}/{file_name}.zip', 'w')
            archive.write(oldest_file)
            os.remove(oldest_file)
        else:
            logger.warning("No files to compress!")

    def compress_old_files(self):
        for directory, sub_dirs, files in os.walk(self.storage_path):
            for file in files:
                file_name, file_extension = os.path.splitext(file)
                if file_extension in (".wav", ".mp3"):
                    _, year, month, day, file_name = file_name.split('/')
                    if datetime.date.today() - datetime.date(year, month, day) >= datetime.timedelta(days=90):
                        logger.info(f"compress {file}")
                        archive = ZipFile(f'{self.archive_path}/{year}/{month}/{day}/{file_name}.zip', 'w')
                        archive.write(file)
                        os.remove(file)

    async def compress_by_space_task(self):
        while True:
            while self.check_no_disk_space_left():
                self.compress_oldest_file()
            await asyncio.sleep(10)

    async def compress_by_date_task(self):
        while True:
            self.compress_old_files()
            await asyncio.sleep(60 * 60 * 24)

    async def run(self):
        await asyncio.gather(self.compress_by_date_task(), self.compress_by_space_task())


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("storage_path")
    parser.add_argument("archive_path")
    args = parser.parse_args()
    ar_srv = ArchiveService(args.storage_path, args.archive_path)
    await ar_srv.run()


if __name__ == '__main__':
    asyncio.run(main())
