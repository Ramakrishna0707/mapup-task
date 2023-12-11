import pandas as pd
from datetime import datetime , timedelta

df1 = pd.read_csv("datasets/dataset-1.csv")
df2 = pd.read_csv("datasets/dataset-2.csv")

def generate_car_matrix(df1)->pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    # Write your logic here
    df_pivot = df1.pivot(index = 'id_1',columns ='id_2',values = 'car').fillna(0)

    return df_pivot
car_matrix = generate_car_matrix(df1)
print(car_matrix)

   

def get_type_count(df1)->dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    # Write your logic here
    bins = [0,15,25,float("inf")]
    labels = ["low",'medium','high']

    df1['car_type'] = pd.cut(df1['car'],bins =bins, labels = labels, include_lowest = True)

    type_count = df1['car_type'].value_counts().to_dict()

    return type_count

type_count = get_type_count(df1)

sorted_type_count = {k: v for k, v in sorted(type_count.items(), key=lambda item: item[0])}

# Print the result
print(sorted_type_count)

def get_bus_indexes(df1)->list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    # Write your logic here
    mean_bus = df1['bus'].mean()
    
    bus_indexes = df1[df1['bus']>2 * mean_bus].index.to_list()

    return bus_indexes

bus_indexes = get_bus_indexes(df1)
print(bus_indexes)


def filter_routes(df1)->list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    # Write your logic here

    filtered_routes = df1.groupby('route')['truck'].mean().reset_index()
    filtered_routes = filtered_routes[filtered_routes['truck']>7]

    return sorted(filtered_routes['route'].to_list())

filtered_routes = filter_routes(df1)
print(filtered_routes)


def multiply_matrix(matrix)->pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    # Write your logic here

    matrix = matrix.applymap(lambda x: x * 0.75 if x > 20 else x*1.25 if x == 20 else x*1.25 )

    matrix = matrix.round(1)

    return matrix

car_matrix = generate_car_matrix(df1)
multiplied_matrix = multiply_matrix(car_matrix)
print(multiplied_matrix)

def time_check(df2)->pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
    # Write your logic here
    df2['start_datetime'] = pd.to_datetime(df2['startDay'] + ' '+ df2['startTime'])
    df2['end_datatime'] = pd.to_datetime(df2['endDay'] +' '+ df2['endTime'])

    df2['start_datetime'] = pd.to_datetime(df2['start_datetime_str'], format='%A %H:%M:%S')
    df2['end_datetime'] = pd.to_datetime(df2['end_datetime_str'], format='%A %H:%M:%S')


    def within_24_hours(timestamp):
         return timestamp.time() >= datetime.strptime('00:00:00', '%H:%M:%S').time() and \
               timestamp.time() <= datetime.strptime('23:59:59', '%H:%M:%S').time()
    
    def within_7_days(day):
        return day.lower() in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    
    boolean_series = (
        (df2['start_datetime'].apply(within_24_hours) & df2['end_datetime'].apply(within_24_hours)) &
        df2['startDay'].apply(within_7_days) & df2['endDay'].apply(within_7_days)
    )

    result = boolean_series.groupby([df2['id'], df2['id_2']]).all()

    df2.drop(['start_datetime', 'end_datetime'], axis = 1, inplace = True)

    return result

result = time_check(df2)
print(result)