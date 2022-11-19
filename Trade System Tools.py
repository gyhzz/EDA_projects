import os
import mysql.connector
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import openpyxl
from datetime import date


connection = mysql.connector.connect(
    host='localhost',
    user='root',    # please use the correct user name
    password='10RelationalDatabasesAreVeryUseful!',
    database='Trading_Platform',
    auth_plugin='mysql_native_password'
)
if connection.is_connected():
    db_Info = connection.get_server_info()
    # print("Connected to MySQL Server version ", db_Info, '\n')

cursor = connection.cursor()


####################
## Menu Functions ##
####################


def main_menu():

    print('''
###############################
########## Main Menu ##########
###############################

Trade System Tools
------------------

1. Query
2. Export Trade Data
3. Reporting
4. Exit Program
''')

    option = get_menu_selection('main')
    dispatch = {
        1: query_menu,
        2: export_trade_data,
        3: reporting_menu,
        4: exit_program
    }
    return dispatch[int(option)]()


def query_menu():

    while True:

        print('''
################################
########## Query Menu ##########
################################

Run Queries
-----------

1. List All Brokers
2. List All Shares
3. Lookup Trade
4. Search Table
5. Return to Main Menu
''')

        option = get_menu_selection('query')
        dispatch = {
            1: list_all_brokers,
            2: list_all_shares,
            3: lookup_trade,
            4: search_trade,
            5: main_menu
        }
        dispatch[int(option)]()


def reporting_menu():

    while True:

        print('''
####################################
########## Reporting Menu ##########
####################################

Select Reports
-----------

1. Trades Made Per Broker (Histogram)
2. Share Price Histories (Time Series)
3. Distribution of Trades Across Stock Exchanges (Pie Chart)
4. Return to Main Menu
''')

        option = get_menu_selection('reporting')
        dispatch = {
            1: trades_per_broker_hist,
            2: share_price_history,
            3: trade_proportion,
            4: main_menu
        }
        dispatch[int(option)]()


def exit_program():
    '''
    Close cursor and connection

    Exit program
    '''

    print("\n>>>>> Goodbye!\n")
    cursor.close()
    connection.close()
    exit()


def get_menu_selection(menu):
    '''
    Function takes one argument (menu) that states which menu it is being executed
    to determine possible options

    User is prompt continuously until a valid option is provided

    Function returns option
    '''

    if menu == 'query':
        choices = '12345'
    else:
        choices = '1234'

    while True:
        selection = input('Selection: ')
        print()

        try:
            assert len(
                selection) == 1 and selection in choices, '\n(!!!!!) Invalid Selection!\n'

        except AssertionError as errMsg:
            print(errMsg)
            continue

        else:
            break

    return selection


#####################
## Query Functions ##
#####################


def list_all_brokers():
    '''
    Function generates SQL query to list all brokers and executes it

    Prints SQL output as a pandas DataFrame to stdout
    '''

    # Store Data
    sql = 'SELECT * FROM brokers;'
    data = execute_query(sql)

    # If no data returned from SQL, inform user
    if len(data) == 0:
        print("\n>>>>> No data found\n")
        return 1

    # Column names
    cols = ['Broker ID', 'First Name', 'Last Name']

    df = convert_to_df(data, cols)
    print(f'\n>>>>> Your Query: {sql}\n')
    print(df)


def list_all_shares():
    '''
    Function generates SQL query to list all shares and executes it

    Prints SQL output as a pandas DataFrame to stdout
    '''

    # Store Data
    sql = """
    SELECT c.name, c.company_id, s.share_id, s.currency_id, c.place_id
    FROM shares s
    INNER JOIN companies c
    ON s.company_id = c.company_id"""
    data = execute_query(sql)

    # If no data returned from SQL, inform user
    if len(data) == 0:
        print("\n>>>>> No data found\n")
        return 1

    # Column names
    cols = ['Company', 'Company ID', 'Share ID', 'Currency ID', 'Place ID']
    df = convert_to_df(data, cols)
    print(f'\n>>>>> Your Query: {sql}\n')
    print(df)


def lookup_trade():
    '''
    Function prompts user for trade_id
    SQL query is generated based on user input and executed
    Prints SQL output as a pandas DataFrame to stdout

    Note: trade_id input value must be a valid digit
    '''

    while True:

        trade_id = input(
            "Please enter ONE OR MORE Trade IDs in the format (1 2 3): ")

        try:
            assert len(
                trade_id) > 0, '\n(!!!!!) Please enter AT LEAST ONE Trade ID!\n'
            trade_id_list = trade_id.split(' ')

            for id in trade_id_list:
                assert id.isdigit(), '\n(!!!!!) Please enter digit(s) for Trade ID!\n'

        except AssertionError as errMsg:
            print(errMsg)
            continue

        else:
            break

    sql = 'SELECT * FROM trades'
    # Loop through multiple values provided for share_id
    count = 0
    for id in trade_id_list:
        condition = f"trade_id = {id}"

        if count == 0:
            sql = append_sql(sql, condition, "AND", True)
        else:
            sql = append_sql(sql, condition, 'OR', False)

        count += 1
    sql += ')'

    print(f'\n>>>>> Your Query: {sql}\n')
    data = execute_query(sql)

    # If no data returned from SQL, inform user
    if len(data) == 0:
        print("\n>>>>> No data found\n")
        return 1

    # Column names
    cols = ['Trade ID', 'Share ID', 'Broker ID', 'Stock Ex ID',
            'Transaction Time', 'Share Amount', 'Price Total']
    df = convert_to_df(data, cols)
    print(df)


def search_trade():
    '''
    Function prompts user to specify ONE OR MORE of share_id, broker_id, and date_range
    SQL query is generated based on user input and executed
    Prints SQL output as a pandas DataFrame to stdout

    Note: At least ONE value must be provided and all values must valid
    '''

    while True:

        print('\nPlease enter AT LEAST ONE of the following: Share ID, Broker ID, Date Range. Press ENTER to skip.\n')

        while True:

            share_id = input(
                "Please enter ONE OR MORE Share IDs in the format (1 2 3): ")

            if len(share_id) == 0:
                share_id_list = ''
                break

            try:
                share_id_list = share_id.split(' ')

                for id in share_id_list:
                    assert id.isdigit(), '\n(!!!!!) Please enter digit(s) for Share ID or ENTER to skip!\n'

            except AssertionError as errMsg:
                print(errMsg)
                continue

            else:
                break

        while True:

            broker_id = input(
                "Please enter ONE OR MORE Broker IDs in the format (1 2 3): ")

            if len(broker_id) == 0:
                broker_id_list = ''
                break

            try:
                broker_id_list = broker_id.split(' ')

                for id in broker_id_list:
                    assert id.isdigit(), '\n(!!!!!) Please enter digit(s) for Broker ID or ENTER to skip!\n'

            except AssertionError as errMsg:
                print(errMsg)
                continue

            else:
                break

        date_range = input("Date Range (DDMMYYYY - DDMMYYYY): ")

        try:
            # Check if at least one value given
            attributes = ('share_id', 'broker_id', 'date_range')
            input_values = (share_id_list, broker_id_list, date_range)
            given_values = {}

            for index in range(3):
                if len(input_values[index]) != 0:
                    given_values[attributes[index]] = input_values[index]

            assert len(given_values.keys()
                       ) > 0, '\n(!!!!!) Not enough details provided!\n'

            # Store Data and add appropriate SQL conditions
            sql = f'SELECT * FROM trades'
            conditions = 0

            for key, value in given_values.items():

                # Generate SQL condition strings based on user values
                if key == 'date_range':
                    start_date = value.split(' - ')[0]
                    end_date = value.split(' - ')[1]
                    start = f"{start_date[4:]}-{start_date[2:4]}-{start_date[:2]}"
                    end = f"{end_date[4:]}-{end_date[2:4]}-{end_date[:2]}"
                    condition = f"transaction_time >= '{start}' AND transaction_time <= '{end}'"

                    sql = append_sql(sql, condition, 'date')
                else:
                    # Loop through multiple values provided for share_id and broker_id
                    count = 0
                    for id in value:
                        condition = f"{key} = {id}"

                        if count == 0:
                            sql = append_sql(sql, condition, "AND", True)
                        else:
                            sql = append_sql(sql, condition, 'OR', False)

                        count += 1

                sql += ')'
                conditions += 1
                if conditions == len(given_values.keys()):
                    sql += ';'

            data = execute_query(sql)
            # If no data returned from SQL, inform user
            if len(data) == 0:
                print(f'\n>>>>> Your Query: {sql}\n')
                print("\n>>>>> No data found\n")
                return 1

        except AssertionError as errMsg:
            print(errMsg)

        except:
            print("\n(!!!!!) Invalid values provided!\n")

        else:
            break

    # Column names
    cols = ['Trade ID', 'Share ID', 'Broker ID', 'Stock Ex ID',
            'Transaction Time', 'Share Amount', 'Price Total']
    df = convert_to_df(data, cols)
    print(f'\n>>>>> Your Query: {sql}\n')
    print(df)


def export_trade_data():
    '''
    Function prompts user to specify ZERO OR MORE of share_id, broker_id, and date_range
    Selection is made by entering the corresponding digits from the menu
    SQL query is generated based on user input and executed
    Exports SQL output as a pandas DataFrame to a .xlsx in working directory

    Note: ZERO values can be specied but all values specified must be valid
    '''

    while True:

        while True:

            selection = input('''
################################################
########## Export Trade Data to Excel ##########
################################################

Enter ZERO OR MORE filters (123), or enter 4 to return to Main Menu

1. Fetch Trades by Share ID
2. Fetch Trades by Broker ID
3. Fetch Trades by Date Range
4. Return to Main Menu

Selection: ''')
            print()

            # Check if selection is not digit, has 4 with other options, is a valid option
            try:
                if not selection.isdigit() and not len(selection) == 0:
                    raise Exception(
                        "\n(!!!!!) Invalid Selection! Enter ZERO OR MORE filters (123), or enter 4 to return to Main Menu.\n")

                if '4' in selection and len(selection) > 1:
                    raise Exception(
                        "\n(!!!!!) Invalid Selection! Enter ZERO OR MORE filters (123), or enter 4 to return to Main Menu.\n")

                for char in selection:
                    if char not in '1234':
                        raise Exception(
                            "\n(!!!!!) Invalid Selection! Enter ZERO OR MORE filters (123), or enter 4 to return to Main Menu.\n")

            except Exception as errMsg:
                print(errMsg)
                continue

            else:
                break

        # Default SQL query and file name
        sql = 'SELECT * FROM trades'
        filename = 'trade_details'
        condition = ''

        if selection == '4':
            return main_menu()

        # Read selections and add the appropriate condition to the current SQL string and update file name
        # Add share_id condition(s) if specified by user
        if '1' in selection:

            while True:

                share_id = input(
                    "Please enter ONE OR MORE Share IDs in the format (1 2 3): ")

                try:
                    share_id_list = share_id.split(' ')
                    for id in share_id_list:
                        assert id.isdigit(), '\n(!!!!!) Please enter digit(s) for Share ID!\n'

                except AssertionError as errMsg:
                    print(errMsg)
                    continue

                else:
                    break

            # Loop through multiple values provided for share_id
            count = 0
            for id in share_id_list:
                condition = f"share_id = {id}"

                if count == 0:
                    sql = append_sql(sql, condition, "AND", True)
                else:
                    sql = append_sql(sql, condition, 'OR', False)

                count += 1
            sql += ')'
            filename += f"_share_id_{'_'.join(share_id_list)}"

        # Add broker_id condition(s) if specified by user
        if '2' in selection:

            while True:

                broker_id = input(
                    "Please enter ONE OR MORE Broker IDs in the format (1 2 3): ")

                try:
                    broker_id_list = broker_id.split(' ')
                    for id in broker_id_list:
                        assert id.isdigit(), '\n(!!!!!) Please enter digit(s) for Broker ID!\n'

                except AssertionError as errMsg:
                    print(errMsg)
                    continue

                else:
                    break

            # Loop through multiple values provided for broker_id
            count = 0
            for id in broker_id_list:
                condition = f"broker_id = {id}"

                if count == 0:
                    sql = append_sql(sql, condition, "AND", True)
                else:
                    sql = append_sql(sql, condition, 'OR', False)

                count += 1

            sql += ')'
            filename += f"_broker_id_{'_'.join(broker_id_list)}"

        # Add date condition if specified by user
        if '3' in selection:

            while True:

                date_range = input(
                    "Please enter a Date Range in the format (DDMMYYYY - DDMMYYYY): ")

                try:
                    start_date = date_range.split(' - ')[0]
                    end_date = date_range.split(' - ')[1]

                    start = f"{start_date[4:]}-{start_date[2:4]}-{start_date[:2]}"
                    end = f"{end_date[4:]}-{end_date[2:4]}-{end_date[:2]}"
                    condition = f"transaction_time >= '{start}' AND transaction_time <= '{end}'"

                    sql_test = f"SELECT * FROM trades WHERE {condition};"
                    execute_query(sql_test)

                except:
                    print(
                        '\n(!!!!!) Please enter a valid Date Range! (DDMMYYYY - DDMMYYYY)\n')
                    continue
                else:
                    break

            sql = append_sql(sql, condition, 'date')
            sql += ')'
            filename += f'_date_range_{start_date}_{end_date}'

        sql += ';'
        filename += '.xlsx'
        print(f'\n>>>>> Your Query: {sql}\n')
        data = execute_query(sql)

        # Column names
        cols = ['Trade ID', 'Share ID', 'Broker ID', 'Stock Ex ID',
                'Transaction Time', 'Share Amount', 'Price Total']
        # Generate DataFrame
        df = convert_to_df(data, cols)

        # Export to Excel and launch
        df.to_excel(filename, index=False)
        print(f"\n>>>>> File Export Successful! Filename: {filename}\n")
        print(f"\n>>>>> Launching {filename}...\n")
        os.system(f"start EXCEL.EXE {filename}")


#########################
## Reporting Functions ##
#########################


def trades_per_broker_hist():
    '''
    Queries for the number of trades made by each broker_id

    Generates and displays a bar chart to show number of trades made by each broker_id
    '''

    # Preparing DataFrame
    cols = ['Broker ID', "Trades"]
    data = execute_query(
        'SELECT broker_id, COUNT(*) FROM trades GROUP BY broker_id;')
    df = convert_to_df(data, cols)

    # Create bars
    height = df['Trades']
    bars = df['Broker ID']
    y_pos = np.arange(len(bars))
    plt.bar(y_pos, height)

    # Create title
    plt.title("Number of Trades by Each Broker")

    # Create names on the x-axis
    plt.xticks(y_pos, bars)

    # Add labels
    addlabels(y_pos, height)

    # Add axis labels
    plt.xlabel("Broker ID")
    plt.ylabel("Number of Trades")

    # Present plot
    plt.show()


def share_price_history():
    '''
    Prompts user for a valid share_id value

    Displays price history of share_id from earliest recorded time to current time
    '''

    while True:

        share_id = input("\nEnter share_id to query: ")

        try:

            # Check if share_id provided is digit
            assert share_id.isdigit(), ("\n(!!!!!) Please enter a digit for Share ID!\n")

            # Check if share_id is a valid share_id
            valid_share_ids = convert_to_df(execute_query(
                'SELECT share_id FROM shares_prices GROUP BY share_id;'), ['share_id']).values.tolist()

            # Convert list of list of items into list of items
            valid_share_ids = [
                item for sublist in valid_share_ids for item in sublist]

            assert int(
                share_id) in valid_share_ids, ("\n(!!!!!) Share ID does not exist!\n")

        except AssertionError as errMsg:
            print(errMsg)
            continue

        else:
            break

    # Preparing DataFrame
    cols = ('Share ID', 'Price', 'Time Start', 'Time End')
    data = execute_query(
        f'SELECT share_id, price, time_start, time_end FROM shares_prices WHERE share_id = {share_id} ORDER BY time_start ASC')
    df = convert_to_df(data, cols)

    # Get earliest time, lowest price, and highest price for chart axis limits
    earliest_time = df['Time Start'][0]
    lowest_price = df['Price'].min()
    highest_price = df['Price'].max()

    # Create plot, set x and y values, set x axis range and y axis range
    fig, ax = plt.subplots()
    ax.plot(df['Time Start'], df['Price'])
    fig.autofmt_xdate()
    ax.set_xlim([date(earliest_time.year, earliest_time.month,
                earliest_time.day), date.today()])
    ax.set_ylim([lowest_price * 0.90, highest_price * 1.05])

    # Create title
    plt.title(f"Price History of Share ID {share_id}")

    # Add axis labels
    plt.xlabel("Date")
    plt.ylabel("Share Price")

    # Display plot
    plt.show()


def trade_proportion():
    '''
    Query for distribution of trades across stock exchanges and store data into DataFrame

    Generate Pie chart to show data
    '''

    # Query and convert output into DataFrame
    sql = """
SELECT s.name, COUNT(t.trade_id)
FROM trades t
INNER JOIN stock_exchanges s
ON t.stock_ex_id = s.stock_ex_id
GROUP BY s.name;"""

    df = convert_to_df(execute_query(sql), ['Stock Exchanges', 'Trades'])

    # Chart Data
    y = df['Trades']
    mylabels = df['Stock Exchanges']

    # Create title
    plt.title(f"Distribution of Trades Across Stock Exchanges")

    # Create and show plot
    plt.pie(y, labels=mylabels)
    plt.show()


#####################
## Other Functions ##
#####################


def append_sql(sql, condition, operator, first=True):
    '''
    Function takes 4 arguments, current SQL string, condition to be added,
    operator or condition ('AND' or 'OR'),
    and whether it is the first condition of the same attribute (to add brackets or not)
    Returns new SQL string with condition added appropriately
    '''

    # Check if current SQL string already has a WHERE clause. If so, append AND instead
    if 'WHERE' not in sql:
        sql += f" WHERE ("
    elif operator != 'date':
        if first:
            sql += f" {operator} ("
        else:
            sql += f" {operator} "
    else:
        sql += f" AND ("

    sql += condition
    return sql


def execute_query(query):
    '''
    Takes a single SQL query string, executes, and returns SQL output as a list of rows
    '''

    # Execute query and return all rows of output
    cursor.execute(query)
    return cursor.fetchall()


def convert_to_df(data, headers):
    '''
    Takes SQL query output's list of rows (data) and a list of column names (headers) as argument
    Returns a pandas DataFrame object

    Note: The number of columns in data and headers must be equal
    '''

    # Create and return DataFrame
    df = pd.DataFrame(data, columns=headers)
    return df


def addlabels(x, y):
    '''
    Adds y labels to plot
    '''

    for i in range(len(x)):
        plt.text(i, y[i], y[i])


def main():

    while True:

        main_menu()


##########
## Main ##
##########

main()
