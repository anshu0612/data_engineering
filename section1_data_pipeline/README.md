
## Databases

### Overview of the files: 
<!-- toc -->
- `dags/transform_data_dag.py`: Consists of airflow DAG 
- `data`: 
   - 'raw_data'
   - 'processed_data'
- `spark`: python files containing dag tasks 
  - `merge_cols_and_save_file.py`
  - `read_file_and_drop_nan_names.py`
  - `remove_prepended_zeroes_add_above_100.py`
  - `split_names.py`
- `test.ipynb`: Test noteboook for the pyspark sql queries
<!-- tocstop -->

### Data Pipeline
<img src="https://miro.medium.com/max/1072/1*TzRyGCOSa4aZhda3B2H-qg.png" width="250"/>

