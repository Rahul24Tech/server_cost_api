import traceback

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
from urllib.parse import quote
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import json
import pytz
from datetime import datetime, timedelta
from decimal import Decimal

# ch_email_db_conx_details = {
#     "RR_gmail": {
#         "host": "10.160.0.4",
#         "port": 13040,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "RR_yahoo": {
#         "host": "10.160.0.4",
#         "port": 13140,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "RR_aol": {
#         "host": "10.160.0.4",
#         "port": 13240,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "RR_hotmail": {
#         "host": "10.160.0.4",
#         "port": 13340,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AA_gmail": {
#         "host": "10.160.0.3",
#         "port": 10040,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AA_yahoo": {
#         "host": "10.160.0.3",
#         "port": 10140,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AA_aol": {
#         "host": "10.160.0.3",
#         "port": 10240,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AA_hotmail": {
#         "host": "10.160.0.3",
#         "port": 10340,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AM_gmail": {
#         "host": "10.160.0.8",
#         "port": 11040,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AM_yahoo": {
#         "host": "10.160.0.8",
#         "port": 11140,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AM_aol": {
#         "host": "10.160.0.8",
#         "port": 11240,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "AM_hotmail": {
#         "host": "10.160.0.8",
#         "port": 11340,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "BB_gmail": {
#         "host": "10.160.0.7",
#         "port": 12040,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "BB_yahoo": {
#         "host": "10.160.0.7",
#         "port": 12140,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "BB_aol": {
#         "host": "10.160.0.7",
#         "port": 12240,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "BB_hotmail": {
#         "host": "10.160.0.7",
#         "port": 12340,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "FX_gmail": {
#         "host": "10.160.0.9",
#         "port": 14040,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "FX_yahoo": {
#         "host": "10.160.0.9",
#         "port": 14140,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "FX_aol": {
#         "host": "10.160.0.9",
#         "port": 14240,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     },

#     "FX_hotmail": {
#         "host": "10.160.0.9",
#         "port": 14340,
#         "db_user": "default",
#         "db_passwd": "",
#         "db_name": "email"
#     }
# }

# ch_email_db_conns = {}
# # Creating connections to ch dbs
# for team_isp, details in ch_email_db_conx_details.items():
#     host = details["host"]
#     port = details["port"]
#     db_user = details["db_user"]
#     db_passwd = details["db_passwd"]
#     db_name = details["db_name"]
#     database_uri = f'clickhouse+native://{db_user}:{quote(db_passwd)}@{host}:{port}/{db_name}'
#     team_isp_db_engine = create_engine(database_uri, echo=False, pool_size=2, max_overflow=0, pool_recycle=3600,
#                                        pool_pre_ping=True)
#     ch_email_db_conns[team_isp] = sessionmaker(team_isp_db_engine)()
# # Checking connection to all ch dbs
# ch_cont_check_query = "SELECT today()"
# for t_ISP in ch_email_db_conns:
#     try:
#         query_out = ch_email_db_conns[t_ISP].execute(ch_cont_check_query).fetchone()
#     except SQLAlchemyError as e:
#         ch_email_db_conns[t_ISP].rollback()
#         error = f"SQLAlchemyError clickhouse while checking connection to {t_ISP} : {e}"
#         raise Exception(error)
#     except Exception as e:
#         error = f"Exception occurred clickhouse while checking connection to {t_ISP} : {e}"
#         raise Exception(error)
# print("Clickhouse connections created successfully")

pgdb_conx_details = {
    "mailing-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "mailing"
    },
    "sponsor-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "sponsor"
    },
    "msa-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "msa"
    },
    "employee-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "employee"
    },
    "oauth-db-pri": {
        "host": "10.160.0.6",
        "port": 5430,
        "db_user": "postgres",
        "db_passwd": "as87gvFa72fGuf2",
        "ssl_mode": "disable",
        "db_name": "oauth_db"
    }
}
start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
print(f"Task started at: {str(start_time)}")
# today = start_time.date()
# today_day_number = today.day
# today_week_no = today.weekday()
# today_str = str(today)
# yesterday = today - timedelta(days=1)
# yesterday_str = str(yesterday)
# yesterday = datetime.strptime(yesterday_str, "%Y-%m-%d")
# where_str_yesterday = f" where timestamp >= '{yesterday}' and timestamp < '{today}' "
# # Calculate the date 30 days ago
# thirty_days_ago = today - timedelta(days=30)

# # Format the dates as strings

# thirty_days_ago_str = thirty_days_ago.strftime('%Y-%m-%d')

# last_thirty_days_record = f" where timestamp >= '{thirty_days_ago_str}' and timestamp < '{today}' "

database_uri = f'postgresql://' \
               f'{pgdb_conx_details["employee-db-pri"]["db_user"]}:{quote(pgdb_conx_details["employee-db-pri"]["db_passwd"])}' \
               f'@' \
               f'{pgdb_conx_details["employee-db-pri"]["host"]}:{pgdb_conx_details["employee-db-pri"]["port"]}' \
               f'/{pgdb_conx_details["employee-db-pri"]["db_name"]}?' \
               f'sslmode={pgdb_conx_details["employee-db-pri"]["ssl_mode"]}'
employee_db_engine = create_engine(database_uri, echo=False, pool_timeout=5, pool_size=2, max_overflow=0,
                                   pool_pre_ping=True)
employee_db_session = sessionmaker(employee_db_engine)()

database_uri = f'postgresql://' \
               f'{pgdb_conx_details["msa-db-pri"]["db_user"]}:{quote(pgdb_conx_details["msa-db-pri"]["db_passwd"])}' \
               f'@' \
               f'{pgdb_conx_details["msa-db-pri"]["host"]}:{pgdb_conx_details["msa-db-pri"]["port"]}' \
               f'/{pgdb_conx_details["msa-db-pri"]["db_name"]}?' \
               f'sslmode={pgdb_conx_details["msa-db-pri"]["ssl_mode"]}'
msa_db_engine = create_engine(database_uri, echo=False, pool_timeout=5, pool_size=2, max_overflow=0, pool_pre_ping=True)
msa_db_session = sessionmaker(msa_db_engine)()

pg_cont_check_query = text("SELECT now()")
for db_session in [employee_db_session, msa_db_session]:
    try:
        query_out = db_session.execute(pg_cont_check_query).fetchone()

    except SQLAlchemyError as e:
        db_session.rollback()
        error = f"SQLAlchemyError postgres while checking connection to {db_session} : {e}"
        raise Exception(error)

    except Exception as e:
        error = f"Exception occurred postgres while checking connection to {db_session} : {e}"
        raise Exception(error)


# print("Postgres connections created successfully")


def get_all_small_n_big_server_cost():
    try:
        main_small_server_response = dict()
        revenue = dict()
        daily_revenue = dict()
        teamwise_total = dict()
        thirty_day_revenue = dict()
        thirty_revenue_data = dict()
        team_averages = dict()
        thirty_day_big_revenue = {}
        new_server = {}
        thirty_revenue_big_data = dict()
        big_server_revenue = dict()
        team_big_average = {}
        server_active_query = """
                        SELECT STRING_AGG(server_name, ', '), team FROM server  WHERE status != 'cancel' and serv_type = 'small' AND use_purpose = 'mailing' GROUP BY team;
                        """
        server_active_count = msa_db_session.execute(server_active_query).fetchall()

        big_active_query = """
                        SELECT STRING_AGG(server_name, ', '), team FROM server  WHERE status != 'cancel' and serv_type = 'big' AND use_purpose = 'mailing' GROUP BY team;
                        """
        big_active_count = msa_db_session.execute(big_active_query).fetchall()
        # print(server_active_count)
        team_dict = {}

        for team_servers, team_name in server_active_count:
            servers = team_servers.split(', ')
            team_dict[team_name] = len(servers)

        print(team_dict)
        big_team_dict = {}

        for team_servers, team_name in big_active_count:
            servers = team_servers.split(', ')
            big_team_dict[team_name] = len(servers)

        print(big_team_dict)

        final_list = []

        for key in team_dict:
            if key not in ('RR', 'TT'):  # Exclude "RR" and "TT" teams
                big_value = big_team_dict.get(key, 0)
                total_count = team_dict[key] + (big_value * 2)
                final_list.append({"team": key, "server_count": total_count})

        print(final_list)
        # result = f"select count(distinct(emp_id)), team from scope where role='mailer' group by team"
        # r_data_recd = employee_db_session.execute(result).fetchall()
        # data_dict = {team: count for count, team in r_data_recd}
        # print(data_dict, "r_data_recd")
        # big_server_count = f"select server_name from server where status!='cancel' and serv_type='big' and use_purpose='mailing';"
        # big_server_count_query = msa_db_session.execute(big_server_count).fetchall()
        # formatted_data = ','.join("'" + item[0] + "'" for item in big_server_count_query)
        # print(formatted_data)
        # server_query = f"select sum(revenue), server_name from true_user_resp_tbl where server_name in ({formatted_data}) and timestamp >= '{yesterday}' and timestamp < '{today}'  group by server_name"
        # # thirty_days_big_server_revenue_data = f"select sum(revenue), server_name from true_user_resp_tbl where server_name in ({formatted_data}) and  timestamp >= '{start_date}' and timestamp <= '{end_date}'  group by server_name"

        # big_query = """SELECT STRING_AGG(server_name, ', '), team FROM server  WHERE status != 'cancel' AND serv_type = 'big' AND use_purpose = 'mailing' GROUP BY team;"""
        # big_server_active_count = msa_db_session.execute(big_query).fetchall()

        # current_t_isp_list = ('AA_gmail', 'AA_yahoo', 'AA_hotmail', 'AA_aol', 'AM_gmail', 'AM_yahoo', 'AM_hotmail',
        #                       'AM_aol', 'RR_gmail', 'RR_yahoo', 'RR_hotmail', 'RR_aol', 'FX_gmail', 'FX_yahoo',
        #                       'FX_hotmail', 'FX_aol', 'BB_gmail', 'BB_yahoo', 'BB_hotmail', 'BB_aol')

        # temp_query = f"select sum(revenue) from true_user_resp_tbl"
        # temp_query += where_str_yesterday

        # temp_data_query = f"select sum(revenue) from true_user_resp_tbl"

        # print("big_server_active_count", big_server_active_count)
        # # For daily revenue

        # # for team_ISP in current_t_isp_list:
        # #     try:
        # #         custom_cpm_recd = ch_email_db_conns[team_ISP].execute(temp_data_query).fetchall()
        # #         # print("custom_data_recd", custom_data_recd)
        # #         if custom_cpm_recd:
        # #             c_info_list_of_dict = [row for row in custom_cpm_recd]
        # #             thirty_day_revenue[team_ISP] = c_info_list_of_dict

        # #     except SQLAlchemyError as e:
        # #         ch_email_db_conns[team_ISP].rollback()
        # #         error = f"SQLAlchemyError while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)
        # #     except Exception as e:
        # #         error = f"Exception while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)

        # # for team_ISP in current_t_isp_list:
        # #     try:
        # #         custom_cpm_recd = ch_email_db_conns[team_ISP].execute(thirty_days_big_server_revenue_data).fetchall()
        # #         # print("custom_data_recd", custom_data_recd)
        # #         if custom_cpm_recd:
        # #             c_info_list_of_dict = [row for row in custom_cpm_recd]
        # #             thirty_day_big_revenue[team_ISP] = c_info_list_of_dict

        # #     except SQLAlchemyError as e:
        # #         ch_email_db_conns[team_ISP].rollback()
        # #         error = f"SQLAlchemyError while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)
        # #     except Exception as e:
        # #         error = f"Exception while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)

        # # for team_ISP in current_t_isp_list:
        # #     try:
        # #         custom_data_recd = ch_email_db_conns[team_ISP].execute(temp_query).fetchall()
        # #         # print("custom_data_recd", custom_data_recd)
        # #         if custom_data_recd:
        # #             c_info_list_of_dict = [row._asdict() for row in custom_data_recd]
        # #             daily_revenue[team_ISP] = c_info_list_of_dict

        # #     except SQLAlchemyError as e:
        # #         ch_email_db_conns[team_ISP].rollback()
        # #         error = f"SQLAlchemyError while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)
        # #     except Exception as e:
        # #         error = f"Exception while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)

        # # new_server = {}
        # # for team_ISP in current_t_isp_list:
        # #     try:
        # #         custom_data_recd = ch_email_db_conns[team_ISP].execute(server_query).fetchall()
        # #         # print("custom_data_recd", custom_data_recd)
        # #         if custom_data_recd:
        # #             c_info_list_of_dict = [row._asdict() for row in custom_data_recd]
        # #             new_server[team_ISP] = c_info_list_of_dict

        # #     except SQLAlchemyError as e:
        # #         ch_email_db_conns[team_ISP].rollback()
        # #         error = f"SQLAlchemyError while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)
        # #     except Exception as e:
        # #         error = f"Exception while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
        # #         raise Exception(error)

        # print("last big ******************** reveue ", new_server)

        # for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
        #     team_total = sum(
        #         daily_revenue.get(f'{team}_{isp}', [{'sum(revenue)': Decimal('0')}])[0]['sum(revenue)'] for isp in
        #         ['gmail', 'yahoo', 'hotmail', 'aol'])
        #     teamwise_total[team] = team_total

        # for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
        #     team_total = sum(
        #         thirty_day_revenue.get(f'{team}_{isp}', [{'sum(revenue)': Decimal('0')}])[0]['sum(revenue)'] for isp in
        #         ['gmail', 'yahoo', 'hotmail', 'aol'])
        #     thirty_revenue_data[team] = team_total

        # # for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
        # #     team_total = sum(
        # #         thirty_day_big_revenue.get(f'{team}_{isp}', [{'sum(revenue)': Decimal('0')}])[0]['sum(revenue)'] for isp
        # #         in ['gmail', 'yahoo', 'hotmail', 'aol'])
        # #     thirty_revenue_big_data[team] = team_total

        # for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
        #     team_total = sum(
        #         new_server.get(f'{team}_{isp}', [{'sum(revenue)': Decimal('0')}])[0]['sum(revenue)'] for isp in
        #         ['gmail', 'yahoo', 'hotmail', 'aol'])
        #     big_server_revenue[team] = team_total

        # days_difference = (today - thirty_days_ago).days

        # for team, value in thirty_revenue_data.items():
        #     team_averages[team] = round(float(value) / days_difference, 2)

        # # for team, value in thirty_revenue_big_data.items():
        # #     team_big_average[team] = round(float(value) / days_difference, 2)

        # fix_team = ['AA', 'AM', 'BB', 'FX', 'RR', 'TT']


        # print("last 30 days small reveue ", thirty_revenue_data)
        # print("last 30 days big reveue ", thirty_revenue_big_data)
        # print("server_active_count", server_active_count)
        # print("server_active_count[0][0]", server_active_count[0][0])
        # team_lengths = {}

        # for codes, team in server_active_count:
        #     codes_list = codes.split(', ')
        #     team_lengths[team] = len(codes_list)
        # print("server_active_count[0][0]", team_lengths)
        # print("big_server_count", big_server_active_count)
        # team_mailer_count = {}
        # server_active_count_data = {}
        # big_server_active_count_data = {}
        # result = f"select count(distinct(emp_id)), team from scope where role='mailer' group by team;"
        # r_data_recd = employee_db_session.execute(result).fetchall()
        # mailer_count = {team: count for count, team in r_data_recd}

        # print(mailer_count)
        # # Storing results in a dictionary
        # for team, count in r_data_recd:
        #     team_mailer_count[team] = count

        # for team, count in server_active_count:
        #     server_active_count_data[team] = count

        # for team, count in big_server_active_count:
        #     big_server_active_count_data[team] = count

        # active_data = {team: 0 for team in fix_team}
        # big_active_data = {}
        # # Initialize active_data with zeros

        # for count, team in server_active_count_data.items():
        #     if team in active_data:
        #         active_data[team] = count

        # for team in fix_team:
        #     cost = big_server_active_count_data.get(team,
        #                                             0)  # Get the cost from the server dictionary or 0 if not found
        #     big_active_data[team] = cost

        # # small server
        # revenue_data = {}
        # team_servers_big = {}
        # fix_team = ['AA', 'AM', 'BB', 'FX', 'RR', 'TT']
        # for server_name, team in big_server_active_count:
        #     if team in fix_team:
        #         if team not in team_servers_big:
        #             team_servers_big[team] = []
        #         team_servers_big[team].append(server_name)

        # print("team_servers_big", team_servers_big)

        # team_big_server_counts = {}
        # team_small_server_counts = {}
        # teamwise_big_strings = {}

        # for team, server_list in team_servers_big.items():
        #     teamwise_big_strings[team] = server_list[0].split(', ')
        # # Iterate through the data dictionary
        # for team, servers in teamwise_big_strings.items():
        #     team_big_server_counts[team] = len(servers) * 2

        # print("team_big_server_counts", team_big_server_counts)

        # i = 0
        # for server_name, team in big_server_active_count:
        #     if team in fix_team:
        #         value = {
        #             'revenue': int(thirty_revenue_big_data.get(team, Decimal('0'))),
        #             'server_name': server_name,
        #             'team': team
        #         }
        #         revenue_data[i] = value
        #         i += 1

        # print("revenue_data", revenue_data)

        # revenue_data_small = {}
        # fix_team = ['AA', 'AM', 'BB', 'FX', 'RR', 'TT']

        # team_servers = {}  # A dictionary to store server names for each team

        # for server_name, team in server_active_count:
        #     if team in fix_team:
        #         if team not in team_servers:
        #             team_servers[team] = []
        #         team_servers[team].append(server_name)
        # print("team_servers_small", team_servers)
        # teamwise_strings = {}

        # for team, server_list in team_servers.items():
        #     teamwise_strings[team] = server_list[0].split(', ')
        # # print(teamwise_strings)
        # for team, servers in teamwise_strings.items():
        #     team_small_server_counts[team] = len(servers)

        # print("team_small_server_counts", team_small_server_counts)

        # team_total_counts = {}

        # for team in team_small_server_counts:
        #     total_count = team_small_server_counts.get(team, 0) + team_big_server_counts.get(team, 0)
        #     team_total_counts[team] = total_count
        # team_total_counts.pop('RR', None)
        # team_total_counts.pop('TT', None)
        # print("team_total_counts", team_total_counts)
        # team_list = [{'team': team, 'server_count': servers} for team, servers in team_total_counts.items()]


        json_string = json.dumps(final_list)
        return json_string
    except Exception as e:
        error_str = f"Exception while generating report : {str(e)}"
        print(error_str)
        print(traceback.print_exc())