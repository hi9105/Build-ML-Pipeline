import json
import os
import tempfile

import hydra
import mlflow
from omegaconf import DictConfig

# If we want to run only the download step : mlflow run . -P steps=download

# If we want to run the download and the basic_cleaning steps :
# mlflow run . -P steps=download,basic_cleaning

# We can override any other parameter in configuration file using Hydra syntax, by providing it as hydra_options
# parameter. mlflow run . -P steps=download,basic_cleaning -P hydra_options="modeling.random_forest.n_estimators=10
# etl.min_price=50"

_steps = [
    "download",
    "basic_cleaning",
    "data_check",
    "data_split",
    "train_random_forest",
    # NOTE: We do not include this in the steps so it is not run by mistake.
    # You first need to promote a model export to "prod" before you can run this,
    # then you need to run this step explicitly "test_regression_model"
]


# This automatically reads in the configuration
@hydra.main(config_name='config')
def go(config: DictConfig):
    # Set up the wandb experiment. All runs will be grouped under this name
    os.environ["WANDB_PROJECT"] = config["main"]["project_name"]
    os.environ["WANDB_RUN_GROUP"] = config["main"]["experiment_name"]

    # Steps to execute
    steps_par = config['main']['steps']
    active_steps = steps_par.split(",") if steps_par != "all" else _steps

    # Move to a temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:

        # Download step
        """ if "download" in active_steps:
            _ = mlflow.run(
                os.path.join(hydra.utils.get_original_cwd(), "src", "download"), "main",
                parameters={"file_url": config["main"]["file_url"],
                            # "sample": config["etl"]["sample"],
                            "artifact_name": "sample.csv",
                            "artifact_type": "raw_data",
                            "artifact_description": "Raw file as downloaded"}, ) """

        if "download" in active_steps:
            _ = mlflow.run(
                f"{config['main']['components_repository']}/get_data",
                "main",
                parameters={"sample": config["etl"]["sample"],
                            "artifact_name": "sample.csv",
                            "artifact_type": "raw_data",
                            "artifact_description": "Raw file as downloaded"}, )

        if "basic_cleaning" in active_steps:
            _ = mlflow.run(os.path.join(hydra.utils.get_original_cwd(), "src", "basic_cleaning"), "main",
                           # This is necessary because Hydra executes the script in a different directory than root
                           # of folder.
                           parameters={"input_artifact": "sample.csv:latest",
                                       "output_artifact": "clean_sample.csv",
                                       "output_type": "clean_sample",
                                       "output_description": "Data with outliers and null values removed",
                                       "min_price": config['etl']['min_price'],
                                       "max_price": config['etl']['max_price']}, )

        if "data_check" in active_steps:
            _ = mlflow.run(os.path.join(hydra.utils.get_original_cwd(), "src", "data_check"), "main",
                           parameters={"csv": "clean_sample.csv:latest",
                                       "ref": "clean_sample.csv:reference",
                                       "kl_threshold": config["data_check"]["kl_threshold"],
                                       "min_price": config['etl']['min_price'],
                                       "max_price": config['etl']['max_price']}, )

        if "data_split" in active_steps:
            _ = mlflow.run(
                f"{config['main']['components_repository']}/train_val_test_split",
                "main",
                parameters={
                    "input": "clean_sample.csv:latest",
                    "test_size": config["modeling"]["test_size"],
                    "random_seed": config["modeling"]["random_seed"],
                    "stratify_by": config["modeling"]["stratify_by"],
                },
            )

        if "train_random_forest" in active_steps:
            # NOTE: we need to serialize the random forest configuration into JSON
            rf_config = os.path.abspath("rf_config.json")
            with open(rf_config, "w+") as fp:
                json.dump(dict(config["modeling"]["random_forest"].items()), fp)  # DO NOT TOUCH

            _ = mlflow.run(os.path.join(hydra.utils.get_original_cwd(), "src", "train_random_forest"), "main",
                           parameters={"trainval_artifact": "trainval_data.csv:latest",
                                       "val_size": config['modeling']['val_size'],
                                       "random_seed": config['modeling']['random_seed'],
                                       "stratify_by": config['modeling']['stratify_by'],
                                       "rf_config": rf_config,
                                       "max_tfidf_features": config['modeling']['max_tfidf_features'],
                                       "output_artifact": "random_forest_export"}, )

        if "test_regression_model" in active_steps:
            _ = mlflow.run(f"{config['main']['components_repository']}/test_regression_model", "main",
                           parameters={"mlflow_model": "random_forest_export:prod",
                                       "test_dataset": "test_data.csv:latest"}, )


if __name__ == "__main__":
    go()