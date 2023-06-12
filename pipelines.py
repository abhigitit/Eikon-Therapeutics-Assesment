import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import config


class UserExperimentsPipeline:
    users = None
    experiments = None
    compounds = None
    derived_features = None

    def __init__(self):
        self.db_engine = create_engine(config.DATABASE_URL)
        self.db_session = sessionmaker(bind=self.db_engine)()

    def start_etl(self):
        self.users = None
        self.experiments = None
        self.compounds = None
        self.extract_data()
        self.transform_data()
        self.load_data()

    def extract_data(self):
        try:
            self.users = pd.read_csv(config.DATASETS_PATH + "users.csv", sep=r"\s*(?:\t|,)\s*", engine="python")
            self.experiments = pd.read_csv(config.DATASETS_PATH + "user_experiments.csv", sep=r"\s*(?:\t|,)\s*",
                                           engine="python")
            self.compounds = pd.read_csv(config.DATASETS_PATH + "compounds.csv", sep=r"\s*(?:\t|,)\s*", engine="python")
        except Exception as e:
            print("Error occurred while extracting data", e)

    def transform_data(self):
        try:
            self.derived_features = self.users.loc[:, ["user_id", "name"]].copy()
            self.derived_features["total_experiments"] = \
                self.experiments.groupby("user_id")["experiment_id"].count().reset_index(
                    name="total_experiments")["total_experiments"]
            self.derived_features["average_experiment_time"] = \
                self.experiments.groupby("user_id")["experiment_run_time"].mean().reset_index(
                    name="average_experiments_amount")["average_experiments_amount"]
            self.derived_features["common_compound"] = \
                self.experiments.groupby("user_id")["experiment_compound_ids"].apply(
                    lambda x: x.str.split(";").explode().mode()[0]).reset_index(name="common_compound")[
                    "common_compound"]
        except Exception as e:
            print("Error occurred while transforming data", e)

    def load_data(self):
        try:
            self.derived_features.to_sql("user_stats", self.db_engine, if_exists="replace", index=False)
        except Exception as e:
            print("Error occurred while loading data to DB", e)

    def get_user_stats(self, user):
        try:
            if user is not None:
                query = text("SELECT * FROM user_stats WHERE user_id = :user")
                result = self.db_session.execute(query, {"user": str(user)})
                columns = result.keys()
                response = [dict(zip(columns, row)) for row in result]

                if not response:
                    print("No data found for this user id")
                    return {"message": "No data found for this user id"}

                return response
            else:
                print("User id is None")
                return {"message": "User id is None"}
        except Exception as e:
            print("Error occurred while retrieving user stats:", str(e))
            return {"message": "Error occurred while retrieving user stats", "error": str(e)}