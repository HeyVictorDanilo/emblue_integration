from database import main_db

import pysftp
import zipfile
import re
import os

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
        finally:
            os.remove(f"ACTIVIDADDETALLEDIARIOFTP_{self.today}.zip")

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

            if line_words[6] == "Enviado":
                sent_values_list.append(
                    (
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7]
                    )
                )

            if line_words[6] == "Click":
                click_values_list.append(
                    (
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7]
                    )
                )

            if line_words[6] == "Abierto":
                open_values_list.append(
                    (
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7]
                    )
                )

            if line_words[6] == "Desuscripto":
                unsubscribe_values_list.append(
                    (
                        line_words[1],
                        line_words[2],
                        line_words[3],
                        line_words[4],
                        line_words[7]
                    )
                )

            if line_words[6] == "Rebote":
                pass

        if sent_values_list:
            build_insert_sent_query = self.build_insert_query(
                table="em_blue_receive_email_event",
                columns=[
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "subject_campaign",
                    "description",
                ],
                values=sent_values_list,
            )
            print(build_insert_sent_query)

        if click_values_list:
            build_insert_click_query = self.build_insert_query(
                table="em_blue_link_click_event",
                columns=[
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "subject_campaign",
                    "url",
                ],
                values=click_values_list,
            )
            print(build_insert_click_query)

        if open_values_list:
            build_insert_open_query = self.build_insert_query(
                table="em_blue_open_email_event",
                columns=[
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "subject_campaign",
                    "description",
                ],
                values=open_values_list,
            )
            print(build_insert_open_query)

        if unsubscribe_values_list:
            build_insert_unsubscribe_query = self.build_insert_query(
                table="em_blue_unsubscribe_event",
                columns=[
                    "sent_date",
                    "activity_date",
                    "campaign",
                    "subject_campaign",
                    "description",
                ],
                values=unsubscribe_values_list,
            )
            print(build_insert_unsubscribe_query)

    @staticmethod
    def build_insert_query(table: str, columns: List[str], values: List[Any]) -> str:
        return f"""
            INSERT INTO {table}({", ".join([str(c) for c in columns])})
            VALUES {values};
        """

    def execute(self):
        self.download_file()
        self.unzip_local_file()
        self.process_file(file_name=self.find_local_file())


if __name__ == "__main__":
    emblue = Emblue()
    emblue.execute()
