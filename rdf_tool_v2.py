import os
import sqlite3
import time
from tkinter import *

import jaydebeapi
import pandas as pd

home_path = os.environ['HomePath']


def query_rdf(conn):
    print('Querying...(remote server might take a while)')
    cursor_meta = conn.cursor()
    cursor_meta.execute('select distinct TABLE_NAME, COLUMN_NAME, ATTRIBUTE from RDF_META')
    values = cursor_meta.fetchall()
    cursor_dvn = conn.cursor()
    cursor_dvn.execute('select COVERED_DATA_REGION, DATA_RELEASE_DATE from RDF_VERSION_INFO')
    rdf_version = cursor_dvn.fetchall()
    for i in rdf_version:
        dvn = i[1]
        region = i[0]
    if not os.path.exists(home_path + '\\Desktop\\RDF_Statistics\\'):
        os.makedirs(home_path + '\\Desktop\\RDF_Statistics\\')
    csv_file_path = (home_path + '\\Desktop\\RDF_Statistics\\RDF_' + region + '_' + dvn + '_statistics.csv')
    file = open(csv_file_path, mode='w')
    file.write('Attribute,Count' + '\n')
    file.write('@REGION,' + region + '\n')
    file.write('@DVN,' + dvn + '\n')
    cursor_rdf = conn.cursor()
    for line in values:
        cursor_rdf.execute('select count(*) from ' + line[0] + ' where ' + line[1] + ' = ' + '\'' + line[2] + '\'')
        table = cursor_rdf.fetchall()
        for data in table:
            result = (
                line[0] + '.' + line[1] + ' = ' + line[2] + ',' + str(data).replace('(', '').replace(')', '').replace(
                    ',', ''))
            print(result)
            file.write(result + '\n')
    return csv_file_path


def connecting_rdf_sqlite(rdf_file_name):
    conn = sqlite3.connect(rdf_file_name)
    result = query_rdf(conn)
    return result


def connecting_rdf_oracle(jdbc_url, user_name, password):
    conn = jaydebeapi.connect("org.hsqldb.jdbcDriver", jdbc_url,
                              [user_name, password],
                              "./hsqldb.jar", )
    print('Connecting database...')
    result = query_rdf(conn)
    return result


def order_csv(result_file):
    df = pd.read_csv(result_file)
    df = df.sort_values('Attribute', ascending=True)
    df.to_csv((result_file + '.ordered.csv'), index=False)
    return (result_file + '.ordered.csv')


def joined_csv(ordered_file_new, ordered_file_old):
    pd_1 = pd.read_csv(ordered_file_new)
    pd_2 = pd.read_csv(ordered_file_old)
    df = pd.merge(pd_1, pd_2, on='Attribute', suffixes=('_new', '_old'), how='left')
    df.to_csv((ordered_file_new + '.joined.csv'), index=False)
    return (ordered_file_new + '.joined.csv')


def runner():
    try:
        db_type = v.get()
        func_option = func_type.get()
        db1_path = db_new_path.get()
        db1_uid = db_new_uid.get()
        db1_pwd = db_new_passwd.get()
        db2_path = db_old_path.get()
        db2_uid = db_old_uid.get()
        db2_pwd = db_old_passwd.get()

        init_config = open('./config.ini', mode='w', encoding='utf-8')
        init_config.write(db_type + '\n')
        init_config.write(func_option + '\n')
        init_config.write(db1_path + '\n')
        init_config.write(db1_uid + '\n')
        init_config.write(db1_pwd + '\n')
        init_config.write(db2_path + '\n')
        init_config.write(db2_uid + '\n')
        init_config.write(db2_pwd + '\n')
        init_config.close()

        app.destroy()

        if func_option == str(1):
            print('The RDF database path: ' + db1_path)
            if db_type == str(1):
                result = connecting_rdf_sqlite(db1_path)
            elif db_type == str(2):
                result = connecting_rdf_oracle(db1_path, db1_uid, db1_pwd)
            order_csv(result)
            print('Calculation Finished...')
        elif func_option == str(2):
            print('1st RDF database path: ' + db1_path)
            print('2nd RDF database path: ' + db2_path)
            if db_type == str(1):
                result_new = connecting_rdf_sqlite(db1_path)
                result_old = connecting_rdf_sqlite(db2_path)
            elif db_type == str(2):
                result_new = connecting_rdf_oracle(db1_path, db1_uid, db1_pwd)
                result_old = connecting_rdf_oracle(db2_path, db2_uid, db2_pwd)
            joined_csv(order_csv(result_new), order_csv(result_old))
    except Exception as e:
        log = open('./error.log', mode='w')
        print(time.strftime('%Y/%m/%d_%H:%M:%S'), e, file=log)
        pass


if __name__ == '__main__':
    if os.path.exists('./config.ini'):
        init_config = open('./config.ini', mode='r')
        f = init_config.readlines()
        if len(f) > 0:
            db_type = f[0].replace('\n', '')
            func_option = f[1].replace('\n', '')
            db1_path = f[2].replace('\n', '')
            db1_uid = f[3].replace('\n', '')
            db1_pwd = f[4].replace('\n', '')
            db2_path = f[5].replace('\n', '')
            db2_uid = f[6].replace('\n', '')
            db2_pwd = f[7].replace('\n', '')
        else:
            db_type = '1'
            func_option = '1'
            db1_path = ''
            db1_uid = ''
            db1_pwd = ''
            db2_path = ''
            db2_uid = ''
            db2_pwd = ''
    else:
        init_config = open('./config.ini', mode='w')

    # welcome message
    print("************************************************")
    print("** Welcome to RDF statistic report tool ver 2 **")
    print("************************************************")
    print("")
    print("This app will create a folder \"RDF_Statistics\" on your desktop.\n")

    app = Tk()
    app.resizable(0, 0)
    app.title('RDF Statistic Report Tool')
    # app.geometry('600x320+600+300')
    app.grid()

    Label(app, text='Select Database Type:').grid(row=0, column=0, padx=10, pady=10, sticky=W)
    v = StringVar()
    v.set(db_type)
    Radiobutton(app, text="Sqlite", variable=v, value=1).grid(row=0, column=1, sticky=W)
    Radiobutton(app, text="Oracle JDBC", variable=v, value=2).grid(row=0, column=2, sticky=W)

    Label(app, text='Select Function:').grid(row=1, column=0, padx=10, pady=10, sticky=W)
    func_type = StringVar()
    func_type.set(func_option)
    Radiobutton(app, text="Calculate 1 database", variable=func_type, value=1).grid(row=1, column=1, sticky=W)
    Radiobutton(app, text="Compare 2 databases", variable=func_type, value=2).grid(row=1, column=2, sticky=W)

    Label(app, text='RDF Database Path (1/Newer):').grid(row=10, column=0, padx=10, pady=10, sticky=W)
    db_new_path = StringVar()
    e1 = Entry(app, width=66, textvariable=db_new_path)
    e1.grid(row=10, padx=10, pady=10, column=1, columnspan=3, sticky=W)
    e1.insert(0, db1_path)

    Label(app, text='Username:').grid(row=11, column=0, padx=10, pady=10, sticky=E)
    db_new_uid = StringVar()
    e2 = Entry(app, width=16, textvariable=db_new_uid)
    e2.grid(row=11, padx=10, pady=10, column=1, columnspan=3, sticky=W)
    e2.insert(0, db1_uid)

    Label(app, text='Password:').grid(row=11, column=2, padx=10, pady=10, sticky=E)
    db_new_passwd = StringVar()
    e3 = Entry(app, width=16, textvariable=db_new_passwd)
    e3.grid(row=11, padx=10, pady=10, column=3, columnspan=3, sticky=W)
    e3.insert(0, db1_pwd)

    Label(app, text='RDF Database Path (2/Older):').grid(row=20, column=0, padx=10, pady=10, sticky=W)
    db_old_path = StringVar()
    e4 = Entry(app, width=66, textvariable=db_old_path)
    e4.grid(row=20, padx=10, pady=10, column=1, columnspan=3, sticky=W)
    e4.insert(0, db2_path)

    Label(app, text='Username:').grid(row=21, column=0, padx=10, pady=10, sticky=E)
    db_old_uid = StringVar()
    e5 = Entry(app, width=16, textvariable=db_old_uid)
    e5.grid(row=21, padx=10, pady=10, column=1, columnspan=3, sticky=W)
    e5.insert(0, db2_uid)

    Label(app, text='Password:').grid(row=21, column=2, padx=10, pady=10, sticky=E)
    db_old_passwd = StringVar()
    e6 = Entry(app, width=16, textvariable=db_old_passwd)
    e6.grid(row=21, padx=10, pady=10, column=3, columnspan=3, sticky=W)
    e6.insert(0, db2_pwd)

    go_button = Button(app, text='\n      GO!      \n', command=runner)
    go_button.grid(row=40, columnspan=6, padx=10, pady=10, sticky=E)

    app.mainloop()
