import yaml
import os
import shutil
import fileinput

config_filepath = "include/dag_config/"
dag_template_filename = "include/dag_template.py"

for filename in os.listdir(config_filepath):
    with open(config_filepath + filename) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        new_filename = "dags/" + config["dag_id"] + ".py"
        shutil.copyfile(dag_template_filename, new_filename)

        with fileinput.input(new_filename, inplace=True) as file:
            for line in file:
                new_line = (
                    line.replace("config_dag_id", "'" + config["dag_id"] + "'")
                    .replace("config_flowref", "'" + config["api_parameters"]["flowref"] + "'")
                    .replace("config_keys", "'" + config["api_parameters"]["series_key"] + "'")
                    .replace("config_start_period", "'" + config["api_parameters"]["start_period"] + "'")
                    .replace("config_end_period", "'" + config["api_parameters"]["end_period"] + "'")
                    .replace("config_table_name", config["schema_parameters"]["table_name"])
                    .replace("config_columns", ',\n'.join(col for col in config["schema_parameters"]["columns"]))
                    .replace("config_copy", ',\n'.join(col.split(' ')[0] for col in config["schema_parameters"]["columns"]))
                )
                print(new_line, end="")
