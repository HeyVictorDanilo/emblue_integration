from database import main_db
import pysftp
from datetime import date
import zipfile
import re
import os

from dotenv import load_dotenv
load_dotenv()

from typing import List, Tuple, Any

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
    def read_local_file() -> str:
        files = [f for f in os.listdir(".") if os.path.isfile(f)]
        for f in files:
            if re.search("\.csv$", f):
                return f


if __name__ == "__main__":
    emblue = Emblue()
    emblue.download_file()
    emblue.unzip_local_file()
    emblue.read_local_file()
