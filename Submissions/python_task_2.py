import pandas as pd
from datetime import time


df = pd.read_csv("datasets/dataset-3.csv")

def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    locations = df['id_start'].unique()
    matrix = pd.DataFrame(0,index = locations, columns= locations)

    for _, row in df.iterrows():
        i = row['id_start']
        j = row['id_end']
        dist = row['distance']

        matrix.at[i,j] = dist
        matrix.at[j,i] = dist

    return matrix

dist_matrix = calculate_distance_matrix(df)
print(dist_matrix)


def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here
    rows = []
    locations = dist_matrix.index

    for i in range(len(locations)):
        for j in range(i+1, len(locations)):
            dist = dist_matrix.iloc[i,j]
            rows.append({
                'id_start' : locations[i],
                'id_end': locations[j],
                'distance': dist
            })
    return pd.DataFrame(rows)

dist_matrix = calculate_distance_matrix(df)
unrolled_df = unroll_distance_matrix(dist_matrix)
print(unrolled_df)

def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here
    ref_rows = df[df['id_start']== reference_id]

    avg_dist = ref_rows['distance'].mean()

    ten_pct = 0.1 * avg_dist
    lower = avg_dist - ten_pct
    upper = avg_dist + ten_pct

    close_ids = df[(df['distance'] >= lower) &
                   (df['distance'] <= upper)] ['id_start'].unique()
    
    close_ids.sort()
    return close_ids

ref_id = 1001402
close_ids = find_ids_within_ten_percentage_threshold(df,ref_id)


def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    rates = {
        'moto' : 0.8,
        'car' : 1.2,
        'rv' : 3.6,
        'truck' : 1.5,
        'bus' : 2.2
    }

    df['moto'] = df['distance'] * rates ['moto']
    df['car'] = df['distance'] * rates['car']
    df['rv'] = df['distance'] * rates['rv']
    df['bus'] = df['distance'] * rates['bus']
    df['truck'] = df['distance'] * rates['truck']


    return df

rates_df = calculate_toll_rate(df)
print(rates_df)

def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    times = [time(0,0,0), time(10,0,0), time(18,0,0), time(23,59,59)]
    
    rows = []
    for start, end in df[['id_start','id_end']].drop_duplicates().values:
        for d1 in days:
            for t1 in times[:-1]:
                d2 = d1 if t1 < times[-2] else days[(days.index(d1)+1)%7]
                t2 = times[times.index(t1)+1]
                discount = 0.8 if d1 in days[:5] and t1 < times[1] else 1.2 
                if d1 in days[5:] or t1 >= times[1]:  
                    discount = 0.7
                for v in ['moto','car','rv','bus','truck']:
                    rate = df.loc[(df['id_start']==start)&(df['id_end']==end), v].squeeze()
                    rows.append({
                        'id_start': start,
                        'id_end': end,
                        'start_day': d1,
                        'start_time': t1, 
                        'end_day': d2,
                        'end_time': t2,
                        v: rate*discount
                    })
                    
    rates_df = pd.DataFrame(rows)
    return rates_df

rates_df = calculate_time_based_toll_rates(df)
print(rates_df)

