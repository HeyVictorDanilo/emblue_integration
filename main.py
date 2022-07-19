from database import main_db

from datetime import date
from typing import List, Tuple, Any
from dotenv import load_dotenv
load_dotenv()

import pysftp
import zipfile
import re
import os
import pandas as pd


class EmblueConnection(pysftp.Connection):
    def __init__(self, *args, **kwargs):
        self._sftp_live = False
        self._transport = None
        super().__init__(*args, **kwargs)


class Emblue:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.today = date.today().strftime("%Y%m%d")

    def get_emblue_accounts(self) -> List[Tuple[Any]]:
        accounts = self.db_instance.handler(query="SELECT * FROM em_blue;")
        return accounts

    def download_file(self) -> None:
        for account in self.get_emblue_accounts():
            with EmblueConnection(
                    account[2], username=account[4], password=account[3]
            ) as sftp:
                with sftp.cd("upload/Report"):
                    sftp.get(f"ACTIVIDADDETALLEDIARIOFTP_{self.today}.zip")

    def unzip_local_file(self) -> None:
        try:
            with zipfile.ZipFile(
                    f"ACTIVIDADDETALLEDIARIOFTP_{self.today}.zip", mode="r"
            ) as archive:
                archive.extractall()
        except zipfile.BadZipFile as error:
            raise error

    @staticmethod
    def find_local_file() -> str:
        files = [f for f in os.listdir(".") if os.path.isfile(f)]
        for f in files:
            if re.search("\.csv$", f):
                return f

    def process_file(self, file_name: str):

        """
        with open(file_name, 'r', encoding='utf-16') as file:
            for line in file.readlines():
                print(line)
        """

        """
        df = pd.read_csv(file_name, encoding="UTF-16", on_bad_lines='skip')
        print(df)
        """

    def execute(self):
        self.download_file()
        self.unzip_local_file()
        self.process_file(file_name=self.find_local_file())


if __name__ == "__main__":
    emblue = Emblue()
    emblue.execute()
