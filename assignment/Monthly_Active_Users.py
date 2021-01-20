import psycopg2 as pg
import pandas as pd


def get_Redshift_connection(hostname, user, pw, port, dbname):
    conn = pg.connect(host = hostname, user = user, password = pw, 
                     port=port, dbname=dbname)
    conn.set_session(readonly=True, autocommit=True)
    return conn.cursor()



hostname = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
redshift_user = "four2qhrm"
redshift_pass = "42Qhrm!1"
port = 5439
dbname = "prod"

cursor = get_Redshift_connection(hostname, redshift_user, redshift_pass, port, dbname)


sql = """
SELECT to_char(ts::date, 'YYYY-MM') AS period, count(DISTINCT userid) as count_result
FROM raw_data.session_timestamp AS A
INNER JOIN raw_data.user_session_channel AS B on A.sessionid = B.sessionid
GROUP BY period;"""

cursor.execute(sql)
df = pd.DataFrame(cursor.fetchall())
df.columns = ['Period', 'Count']

print(df)
