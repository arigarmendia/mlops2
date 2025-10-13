import datetime

from airflow.decorators import dag, task

markdown_text = """
### ETL Process for Rain in Australia dataset

This DAG extracts information from the original CSV file stored in Google Drive. 
It preprocesses the data encoding categorical features and scaling numerical features.

After preprocessing, the data is saved back into a S3 bucket as two separate CSV files: one for training and one for 
testing. The split between the training and testing datasets is 85/15 and they are stratified.
"""


default_args = {
    'owner': "Kevin André Cajachuán Arroyo, Martín Paz, Ariadna Garmendia",
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'dagrun_timeout': datetime.timedelta(minutes=60)
}


@dag(
    dag_id="process_etl_rain_in_australia_data",
    description="ETL process for Rain in Australia data, separating the dataset into training and testing sets.",
    doc_md=markdown_text,
    tags=["ETL", "Rain in Australia"],
    default_args=default_args,
    catchup=False,
    schedule_interval='0 0 1 * *', # Primer día de cada mes a las 0:00
    start_date=datetime.datetime(2024, 10, 1),
    is_paused_upon_creation=True
)
def process_etl_rain_in_australia_data():

    @task.virtualenv(
        task_id="obtain_original_data",
        requirements=["awswrangler==3.6.0"],
        system_site_packages=True
    )
    def get_data():
        """
        Load the raw data from Google Drive
        """
        import awswrangler as wr
        import pandas as pd

        # fetch dataset
        url = "https://drive.google.com/uc?export=download&id=13ooui3hVDHgG0XK0rnekt2T1AUgKSU-z"
        dataframe = pd.read_csv(url)

        data_path = "s3://data/raw/weatherAUS.csv"

        wr.s3.to_csv(df=dataframe,
                     path=data_path,
                     index=False)


    @task.virtualenv(
        task_id="transform_data",
        requirements=["awswrangler==3.6.0"],
        system_site_packages=True
    )
    def transform_data():
        """
        Transform data imputing nulls, encoding categorical features and treating outliers.
        """
        import json
        import datetime
        import boto3
        import botocore.exceptions
        import mlflow

        import awswrangler as wr
        import pandas as pd
        import numpy as np

        from airflow.models import Variable

        # Read original data
        data_original_path = "s3://data/raw/weatherAUS.csv"
        original_dataset = wr.s3.read_csv(data_original_path)

        dataset = original_dataset.copy()

        target_col = Variable.get("target_col")

        # Drop rows with null target
        dataset = dataset.dropna(subset=target_col)

        # Get categorical features
        categories_list = dataset.select_dtypes(include=['object']).columns.tolist()
        categories_list.remove(target_col)

        # Separate Date fields
        dataset['Date'] = pd.to_datetime(dataset['Date'])
        dataset['Year'] = dataset['Date'].dt.year
        dataset['Month'] = dataset['Date'].dt.month
        dataset['Day'] = dataset['Date'].dt.day

        month_to_season = {
            12: 'Summer', 1: 'Summer', 2: 'Summer',
            3: 'Fall', 4: 'Fall', 5: 'Fall',
            6: 'Winter', 7: 'Winter', 8: 'Winter',
            9: 'Spring', 10: 'Spring', 11: 'Spring'
        }

        dataset['Season'] = dataset['Month'].map(month_to_season)
        dataset.drop(columns='Date', inplace=True)

        # Aggregate data in order to impute nulls
        columns_with_nan = dataset.columns[dataset.isna().any()].tolist()
        get_mode = lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan

        for col in columns_with_nan:
            # Fill based on (Day, Month, Location)
            dataset[col] = dataset[col].fillna(
                dataset.groupby(['Day', 'Month', 'Location'])[col].transform(get_mode)
            )
            # Then (Month, Location)
            dataset[col] = dataset[col].fillna(
                dataset.groupby(['Month', 'Location'])[col].transform(get_mode)
            )
            # Then (Location)
            dataset[col] = dataset[col].fillna(
                dataset.groupby(['Location'])[col].transform(get_mode)
            )
            # Finally global mode
            dataset[col] = dataset[col].fillna(dataset[col].mode().iloc[0])

        # Outliers treatment
        numeric_cols = dataset.select_dtypes(include='number').columns
        means = dataset[numeric_cols].mean()
        stds = dataset[numeric_cols].std()
        cutoff = 3 * stds
        lower = means - cutoff
        upper = means + cutoff

        dataset[numeric_cols] = dataset[numeric_cols].clip(lower, upper, axis=1)

        # Encoding RainToday and RainTomorrow
        encode_yes_no = lambda x: int(x == "Yes")
        dataset["RainToday"] = dataset["RainToday"].apply(encode_yes_no)
        dataset["RainTomorrow"] = dataset["RainTomorrow"].apply(encode_yes_no)

        # Encoding wind dir
        dir_to_degree = {
            'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5, 'E': 90, 'ESE': 112.5,
            'SE': 135, 'SSE': 157.5, 'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
            'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
        }

        dir_variables = ["WindGustDir", "WindDir9am", "WindDir3pm"]

        for dir_var in dir_variables:
            dataset[dir_var] = dataset[dir_var].map(dir_to_degree)
            dataset[f'{dir_var}_sin'] = np.sin(np.deg2rad(dataset[dir_var]))
            dataset[f'{dir_var}_cos'] = np.cos(np.deg2rad(dataset[dir_var]))
            dataset.drop(columns=[dir_var], inplace=True)

        # Encoding Location
        coordinates = {
            'Albury': (-36.0804766, 146.9162795), 
            'BadgerysCreek': (-33.8816671, 150.7441627), 
            'Cobar': (-31.4983333, 145.8344444), 
            'CoffsHarbour': (-30.2985996, 153.1094116), 
            'Moree': (-29.4617202, 149.8407153), 
            'Newcastle': (-32.9272881, 151.7812534), 
            'NorahHead': (-33.2816667, 151.5677778), 
            'NorfolkIsland': (-29.0328038, 167.9483137), 
            'Penrith': (-33.74779624999999, 150.71478824217297), 
            'Richmond': (-37.80745, 144.99071761295892), 
            'Sydney': (-33.8698439, 151.2082848), 
            'SydneyAirport': (-33.9498935, 151.18196819346016), 
            'WaggaWagga': (-35.115, 147.3677778), 
            'Williamtown': (-32.815, 151.8427778), 
            'Wollongong': (-34.4243941, 150.89385), 
            'Canberra': (-35.2975906, 149.1012676), 
            'Tuggeranong': (-35.4209771, 149.0921341), 
            'MountGinini': (-35.5297437, 148.7725396), 
            'Ballarat': (-37.5623013, 143.8605645), 
            'Bendigo': (-36.7590183, 144.2826718), 
            'Sale': (-38.1094463, 147.0656717), 
            'MelbourneAirport': (-37.6667554, 144.8288501411705), 
            'Melbourne': (-37.8142454, 144.9631732), 
            'Mildura': (-34.195274, 142.1503146), 
            'Nhil': (-35.4713087, 141.3062355), 
            'Portland': (-38.3456231, 141.6042304), 
            'Watsonia': (-37.7109468, 145.0837808), 
            'Dartmoor': (-27.996161999999998, 115.18921814168053), 
            'Brisbane': (-27.4689682, 153.0234991), 
            'Cairns': (-16.9206657, 145.7721854), 
            'GoldCoast': (-28.0805, 153.4309186971459), 
            'Townsville': (-19.2569391, 146.8239537), 
            'Adelaide': (-34.9281805, 138.5999312), 
            'MountGambier': (-37.8246698, 140.78195963207457), 
            'Nuriootpa': (-34.4693354, 138.9939006), 
            'Woomera': (-31.1999142, 136.8253532), 
            'Albany': (-35.0247822, 117.883608), 
            'Witchcliffe': (-34.0263348, 115.1004768), 
            'PearceRAAF': (-31.6739604, 116.01754351808195), 
            'PerthAirport': (-31.9406095, 115.96760765137932), 
            'Perth': (-31.9558933, 115.8605855), 
            'SalmonGums': (-32.9815167, 121.6440785), 
            'Walpole': (-34.9776796, 116.7310063), 
            'Hobart': (-42.8825088, 147.3281233), 
            'Launceston': (-41.4340813, 147.1373496), 
            'AliceSprings': (-23.6983884, 133.8812885), 
            'Darwin': (-12.46044, 130.8410469), 
            'Katherine': (-14.4646157, 132.2635993), 
            'Uluru': (-25.3455545, 131.03696147470208)
        }

        dataset[['Latitude', 'Longitude']] = dataset['Location'].apply(lambda x: pd.Series(coordinates[x]))
        dataset.drop(columns=['Location'], inplace=True)

        # Encoding Season
        season_to_degree = {'Winter': 0, 'Spring': 90, 'Summer': 180, 'Fall': 270}
        deg = dataset['Season'].map(season_to_degree)
        dataset['Season_sin'] = np.sin(np.deg2rad(deg))
        dataset['Season_cos'] = np.cos(np.deg2rad(deg))
        dataset.drop(columns=["Season"], inplace=True)

        data_end_path = "s3://data/raw/weatherAUS_transformed.csv"
        wr.s3.to_csv(df=dataset,
                     path=data_end_path,
                     index=False)

        # Save information of the dataset
        client = boto3.client('s3')

        data_dict = {}
        try:
            client.head_object(Bucket='data', Key='data_info/data.json')
            result = client.get_object(Bucket='data', Key='data_info/data.json')
            text = result["Body"].read().decode()
            data_dict = json.loads(text)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != "404":
                # Something else has gone wrong.
                raise e

        dataset_log = original_dataset.drop(columns=target_col)
        dataset_processed_log = dataset.drop(columns=target_col)

        # Upload JSON String to an S3 Object
        data_dict['columns'] = dataset_log.columns.to_list()
        data_dict['columns_after_transform'] = dataset_processed_log.columns.to_list()
        data_dict['target_col'] = target_col
        data_dict['categorical_columns'] = categories_list
        data_dict['columns_dtypes'] = {k: str(v) for k, v in dataset_log.dtypes.to_dict().items()}
        data_dict['columns_dtypes_after_transform'] = {k: str(v) for k, v in dataset_processed_log.dtypes.to_dict().items()}
        data_dict['wind_dir_columns'] = dir_variables
        data_dict['wind_dir_degrees'] = dir_to_degree
        data_dict['city_coordinates'] = coordinates
        data_dict['season_degrees'] = season_to_degree

        category_values_dict = {}
        for category in categories_list:
            category_values_dict[category] = np.sort(dataset_log[category].dropna().unique()).tolist()

        data_dict['categories_values_per_categorical'] = category_values_dict

        data_dict['date'] = datetime.datetime.today().strftime('%Y/%m/%d-%H:%M:%S')
        data_string = json.dumps(data_dict, indent=2)

        client.put_object(
            Bucket='data',
            Key='data_info/data.json',
            Body=data_string
        )

        # Log the processed dataset to MLflow
        mlflow.set_tracking_uri('http://mlflow:5000')
        experiment = mlflow.set_experiment("Rain in Australia")

        with mlflow.start_run(run_name='ETL_run_' + datetime.datetime.today().strftime('%Y/%m/%d-%H:%M:%S'),
                              experiment_id=experiment.experiment_id,
                              tags={"experiment": "etl", "dataset": "Rain in Australia"},
                              log_system_metrics=True):
            mlflow.log_param("raw_dataset_path", "s3://data/raw/weatherAUS.csv")
            mlflow.log_param("transformed_dataset_path", "s3://data/raw/weatherAUS_transformed.csv")
            mlflow.log_param("rows_after_transform", dataset.shape[0])
            mlflow.log_param("columns_after_transform", list(dataset.columns))

    @task.virtualenv(
        task_id="split_dataset",
        requirements=["awswrangler==3.6.0",
                      "scikit-learn==1.3.2"],
        system_site_packages=True
    )
    def split_dataset():
        """
        Generate a dataset split into a training part and a test part
        """
        import awswrangler as wr
        from sklearn.model_selection import train_test_split
        from airflow.models import Variable

        def save_to_csv(df, path):
            wr.s3.to_csv(df=df,
                         path=path,
                         index=False)

        data_original_path = "s3://data/raw/weatherAUS_transformed.csv"
        dataset = wr.s3.read_csv(data_original_path)

        test_size = Variable.get("test_size")
        target_col = Variable.get("target_col")

        X = dataset.drop(columns=target_col)
        y = dataset[[target_col]]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, stratify=y, random_state=42)

        # Clean duplicates
        dataset.drop_duplicates(inplace=True, ignore_index=True)

        save_to_csv(X_train, "s3://data/final/train/weatherAUS_X_train.csv")
        save_to_csv(X_test, "s3://data/final/test/weatherAUS_X_test.csv")
        save_to_csv(y_train, "s3://data/final/train/weatherAUS_y_train.csv")
        save_to_csv(y_test, "s3://data/final/test/weatherAUS_y_test.csv")

    @task.virtualenv(
        task_id="normalize_numerical_features",
        requirements=["awswrangler==3.6.0",
                      "scikit-learn==1.3.2",
                      "mlflow==2.10.2"],
        system_site_packages=True
    )
    def normalize_data():
        """
        Standardization of numerical columns
        """
        import json
        import mlflow
        import boto3
        import botocore.exceptions

        import awswrangler as wr
        import pandas as pd

        from sklearn.preprocessing import StandardScaler

        def save_to_csv(df, path):
            wr.s3.to_csv(df=df,
                         path=path,
                         index=False)

        X_train = wr.s3.read_csv("s3://data/final/train/weatherAUS_X_train.csv")
        X_test = wr.s3.read_csv("s3://data/final/test/weatherAUS_X_test.csv")

        sc_X = StandardScaler(with_mean=True, with_std=True)
        X_train_arr = sc_X.fit_transform(X_train)
        X_test_arr = sc_X.transform(X_test)

        X_train = pd.DataFrame(X_train_arr, columns=X_train.columns)
        X_test = pd.DataFrame(X_test_arr, columns=X_test.columns)

        save_to_csv(X_train, "s3://data/final/train/weatherAUS_X_train.csv")
        save_to_csv(X_test, "s3://data/final/test/weatherAUS_X_test.csv")

        # Save information of the dataset
        client = boto3.client('s3')

        try:
            client.head_object(Bucket='data', Key='data_info/data.json')
            result = client.get_object(Bucket='data', Key='data_info/data.json')
            text = result["Body"].read().decode()
            data_dict = json.loads(text)
        except botocore.exceptions.ClientError as e:
            # Something else has gone wrong.
            raise e

        # Upload JSON String to an S3 Object
        data_dict['standard_scaler_mean'] = sc_X.mean_.tolist()
        data_dict['standard_scaler_std'] = sc_X.scale_.tolist()
        data_string = json.dumps(data_dict, indent=2)

        client.put_object(
            Bucket='data',
            Key='data_info/data.json',
            Body=data_string
        )

        mlflow.set_tracking_uri('http://mlflow:5000')
        experiment = mlflow.set_experiment("Rain in Australia")

        # Obtain the last experiment run_id to log the new information
        list_run = mlflow.search_runs([experiment.experiment_id], output_format="list")

        with mlflow.start_run(run_id=list_run[0].info.run_id):

            mlflow.log_param("Train observations", X_train.shape[0])
            mlflow.log_param("Test observations", X_test.shape[0])
            mlflow.log_param("Standard Scaler feature names", sc_X.feature_names_in_)
            mlflow.log_param("Standard Scaler mean values", sc_X.mean_)
            mlflow.log_param("Standard Scaler scale values", sc_X.scale_)


    get_data() >> transform_data() >> split_dataset() >> normalize_data()


dag = process_etl_rain_in_australia_data()