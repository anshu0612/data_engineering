# Data Engineering Technical Assessment

-  data pipelines
-  databases
-  system design
-  charts & APIs
-  machine learning

The individual README.md files are provided for each section. 

## Directory structure 
```
├── data_engineering
│   ├── section1_data_pipeline
│   |    ├── dags
|   |    |    ├── transform_data_dag.py
│   |    ├── data
|   |    |    ├── processed_data
|   |    |    ├── raw_data
│   |    ├── spark
|   |    |    ├── merge_cols_and_save_file.py
|   |    |    ├── read_file_and_drop_nan_names.py
|   |    |    ├── remove_prepended_zeroes_add_above_100.py
|   |    |    ├── split_names.py
│   |    ├── README.md
│   ├── section2_databases
│   |    ├── Dockerfile
│   |    ├── car_sales_db_init.sql
│   |    ├── queries.sql
│   |    ├── er_car_sales_db.png
│   |    ├── README.md
│   ├── section3_system_design
│   |    ├── system_design_aws.pdf
│   |    ├── system_design_aws.png
│   |    ├── README.md
│   ├── section4_charts_apis
│   |    ├── covid19_cases.ipynb
│   |    ├── covid19_cases.pdf
│   |    ├── dash_component_covid19_cases.png
│   |    ├── README.md
│   ├── section5_ml
│   |    ├── predict_car_price.ipynb
│   |    ├── predict_car_price.pdf
│   |    ├── data
|   |    |    ├── car.data
│   |    ├── README.md
├──README.md
