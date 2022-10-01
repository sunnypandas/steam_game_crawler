import csv
import pandas as pd

from steam_game_crawler.utils.httputils import convertRawString2Json


def get_rows(path):
    rows = pd.read_csv(path, header=0, index_col=None, dtype=str, chunksize=10000)
    rows = rows.fillna('')
    return rows

def create_csv(rows, path):
    with open(path, 'a') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def process_steamspy_owner_detail():
    num = 1
    with pd.read_csv(r'/Users/sunpanpan/Workspace/Work/Doyle/steam/steamspy_owner_detail.csv', header=0, index_col=None, dtype=str, chunksize=10000) as reader:
        for chunk in reader:
            chunk = chunk.fillna('')
            for row in chunk.itertuples():
                print(num)
                num = num + 1
                owners_chart_data_list = []
                url = row.url
                owners_chart_data = row.owners_chart_data
                owners = convertRawString2Json(owners_chart_data)
                for owner in owners:
                    date = owner.get('date')
                    val = owner.get('val')
                    event = owner.get('event')
                    link = owner.get('link')
                    color = owner.get('color')
                    val2 = owner.get('val2')
                    val3 = owner.get('val3')
                    owners_chart_data_list.append([url, date, val, event, link, color, val2, val3])
                if len(owners_chart_data_list) > 0:
                    create_csv(owners_chart_data_list, r'/Users/sunpanpan/Workspace/Work/Doyle/steam/steamspy_owner_detail_parse.csv')

def main():
    process_steamspy_owner_detail()

if __name__ == '__main__':
    main()