import logging
import os
import time
from io import BytesIO
from typing import List, Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from smart_open import smart_open

from database import main_db

load_dotenv()


class Emblue:
    def __init__(self, searching_date: str = ""):
        self.db_instance = main_db.DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )

    def get_file_contents(self):
        try:
            response = self.client.list_objects(Bucket=os.getenv("BUCKET_CSV_FILES"))
        except ClientError as e:
            logging.error(e)
        else:
            return response.get("Contents")

    def process_files(self):
        for content in self.get_file_contents():
            with smart_open(f's3://{os.getenv("BUCKET_CSV_FILES")}/{content.get("Key")}', 'rb', encoding="utf-16") as file:
                for line in file:
                    print(line)

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
        self.process_files()
        end = time.time()
        print(end - start)


if __name__ == "__main__":
    emblue = Emblue()
    emblue.execute()
