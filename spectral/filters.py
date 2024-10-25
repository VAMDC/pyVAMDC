def filterDataByColumnValues(input_df, colum_to_filter, minValue = None, maxValue = None):
    """
    Filters a DataFrame to include only rows where the value in the specified column (which must contain numbers)
    satisfies numerical constraints, defined by the call arguments.

    Args:
        input_df (pandas.DataFrame): The input DataFrame.
        column_to_filter (str): The name of the column to filter.
        minValue (float): the inf value for filtering. Filtered results will be greater than this value. Default None.
        maxValue (float): the sup value for filtering. Filtered results will be smaller than this value. Default None.

    Returns:
        pandas.DataFrame: A new DataFrame containing only the filtered rows.
    """
    df_to_return = None
    
    if minValue is not None and maxValue is not None:
        df_to_return = input_df[(input_df[colum_to_filter] >= minValue) & (input_df[colum_to_filter] <= maxValue)]
        return df_to_return
    
    if minValue is not None and maxValue is None:
        df_to_return = input_df[(input_df[colum_to_filter] >= minValue)]
        return df_to_return
    
    if minValue is None and maxValue is not None:
        df_to_return = input_df[(input_df[colum_to_filter] <= maxValue)]
        return df_to_return
    
    return df_to_return


def filterDataHavingColumnContainingStrings(input_df, column_to_filter, substring_list):
    """
    Filters a DataFrame to include only rows where the value in the specified column
    contains at least one of the string in the list given as call argument.

    Args:
        input_df (pandas.DataFrame): The input DataFrame.
        column_to_filter (str): The name of the column to filter.
        substring_list (list): A list of strings to check for substrings.

    Returns:
        pandas.DataFrame: A new DataFrame containing only the filtered rows.
    """
    # Create a boolean mask for the rows to keep
    mask = input_df[column_to_filter].apply(lambda x: any(substring in str(x) for substring in substring_list))

    # Filter the DataFrame using the mask
    filtered_df = input_df[mask]

    return filtered_df



def filterDataHavingColumnNotContainingStrings(input_df, column_to_filter, substring_list):
    """
    Filters a DataFrame to exclude rows where the value in the specified column
    contains any of the substrings in the given list.

    Args:
        input_df (pandas.DataFrame): The input DataFrame.
        column_to_filter (str): The name of the column to filter.
        substring_list (list): A list of strings to exclude.

    Returns:
        pandas.DataFrame: A new DataFrame excluding the rows with the specified substrings.
    """
    # Create a boolean mask for the rows to keep
    mask = ~input_df[column_to_filter].apply(lambda x: any(substring in str(x) for substring in substring_list))

    # Filter the DataFrame using the mask
    filtered_df = input_df[mask]

    return filtered_df
