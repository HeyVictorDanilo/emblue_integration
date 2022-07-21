from database import main_db

import pysftp
import zipfile
import re
import os
import time

from datetime import date
from typing import List, Tuple, Any
from itertools import islice
from dotenv import load_dotenv

load_dotenv()


class EmblueConnection(pysftp.Connection):
    def __init__(self, *args, **kwargs):
        self._sftp_live = False
        self._transport = None
        super().__init__(*args, **kwargs)


class ManageSFTPFile:
    def __init__(self, accounts, file_name):
        self.accounts = accounts
        self.file_name = file_name

    def download_file(self, accounts) -> None:
        for account in self.accounts:
            with EmblueConnection(
                account[2], username=account[4], password=account[3]
            ) as sftp:
                with sftp.cd("upload/Report"):
                    sftp.get(f"{self.file_name}.zip")


class ManageZip:
    def __init__(self, file_name):
        self.file_name = file_name

    def unzip_local_file(self) -> None:
        try:
            with zipfile.ZipFile(
                f"{self.file_name}.zip", mode="r"
            ) as archive:
                archive.extractall()
        except zipfile.BadZipFile as error:
            raise error
        finally:
            os.remove(f"{self.file_name}.zip")


class Emblue:
    def __init__(self, searching_date: str = ""):
        self.db_instance = main_db.DBInstance(public_key=os.getenv("CLIENT_KEY"))
        if searching_date:
            self.today = searching_date
        else:
            self.today = date.today().strftime("%Y%m%d")

    def get_emblue_accounts(self) -> List[Tuple[Any]]:
        accounts = self.db_instance.handler(query="SELECT * FROM em_blue;")
        return accounts

    @staticmethod
    def find_local_file() -> str:
        files = [f for f in os.listdir(".") if os.path.isfile(f)]
        for f in files:
            if re.search("\.csv$", f):
                return f

    def process_file(self, file_name: str):
        with open(file_name, "r", encoding="utf-16") as file:
            while True:
                lines = list(islice(file, 1000))
                self.process_lines(lines=lines)
                if not lines:
                    break

        os.remove(file_name)

    def process_lines(self, lines: List[str]):
        sent_values_list = []
        click_values_list = []
        open_values_list = []
        unsubscribe_values_list = []

        for line in lines:
            line_words = line.split(";")
            if not line_words[8]:
                tag = "NULL"
            else:
                tag = line_words[8]

            if line_words[6] == "Enviado":
                sent_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag
                    )
                )

            if line_words[6] == "Click":
                click_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag
                    )
                )

            if line_words[6] == "Abierto":
                open_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag
                    )
                )

            if line_words[6] == "Desuscripto":
                unsubscribe_values_list.append(
                    (
                        line_words[0],
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7],
                        tag
                    )
                )

            if line_words[6] == "Rebote":
                pass

        if sent_values_list:
            build_insert_sent_query = self.build_insert_query(
                table="em_blue_sent_email_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "description",
                    "tag"
                ],
                values=sent_values_list,
            )
            self.db_instance.handler(query=build_insert_sent_query)

        if click_values_list:
            build_insert_click_query = self.build_insert_query(
                table="em_blue_link_click_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "url",
                    "tag"
                ],
                values=click_values_list,
            )
            self.db_instance.handler(query=build_insert_click_query)

        if open_values_list:
            build_insert_open_query = self.build_insert_query(
                table="em_blue_open_email_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "description",
                    "tag"
                ],
                values=open_values_list,
            )
            self.db_instance.handler(query=build_insert_open_query)

        if unsubscribe_values_list:
            build_insert_unsubscribe_query = self.build_insert_query(
                table="em_blue_unsubscribe_event",
                columns=[
                    "email",
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "action",
                    "description",
                    "tag"
                ],
                values=unsubscribe_values_list,
            )
            self.db_instance.handler(query=build_insert_unsubscribe_query)

    @staticmethod
    def build_insert_query(table: str, columns: List[str], values: List[Any]) -> str:
        return f"""
            INSERT INTO {table}({", ".join([str(c) for c in columns])})
            VALUES {values};
        """.replace("[", "").replace("]", "")

    def execute(self):
        start = time.time()

        ManageSFTPFile(
            accounts=self.get_emblue_accounts(),
            file_name=f"ACTIVIDADDETALLEDIARIOFTP_{self.today}"
        ).download_file()

        ManageZip(
            file_name=f"ACTIVIDADDETALLEDIARIOFTP_{self.today}"
        ).unzip_local_file()

        self.process_file(file_name=self.find_local_file())
        end = time.time()
        print(end - start)


if __name__ == "__main__":
    emblue = Emblue()
    emblue.execute()
