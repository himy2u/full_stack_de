# built-in
import argparse
import json
import os

#3rd party
import psycopg2

class EtlHandler():
    def __init__(self):
        self.conn = psycopg2.connect(dbname='oceanrecords', host='localhost', port=5432, user='etl_usr', password='meget')
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()        

    def stage_data(self, extraction_dir, table_to_stage):
        """
        data on stage should reflect operational system
        flow for staging data:
          - find files to stage
          - truncate dlt;
          - copy to dlt;
          - insert to stage (with deduplication and most recent row);
          - change file status to done;
        """
        self.manage_stage_tables(extraction_dir, table_to_stage)
        tbl_under = table_to_stage.replace('.', '_')
        tbl_extraction_dir = os.path.join(extraction_dir, tbl_under)
        raw_tbl_name = table_to_stage.split('.')[1]
        with open(os.path.join(tbl_extraction_dir, tbl_under+'.aux'), 'r') as aux_file:
            tbl_aux = json.loads(aux_file.read())
        columns = ','.join(tbl_aux['columns'])
        status_fiels = self.get_status_files(extraction_dir, table_to_stage)
        for sfile in status_fiels:
            data_file_path = os.path.join(tbl_extraction_dir, 'data', sfile.split('.')[1]+'.csv')
            copy_cmd = """COPY stage.dlt_{} ({}) 
                          FROM '{}' \nHEADER DELIMITER ',' CSV NULL AS '';
                       """.format(raw_tbl_name, columns, data_file_path)
            print(copy_cmd)
            self.cursor.execute(copy_cmd)
        sql_script = """
                      BEGIN;\n
                      -- delete from stg table rows that are no longer up-to-date
                     DELETE FROM stage.stg_{tbl_name}
                      WHERE ({uniq_col}) IN (SELECT DISTINCT {uniq_col}
                                               FROM stage.dlt_{tbl_name});\n
                      -- populate stg table with new data
                     INSERT INTO stage.stg_{tbl_name} ({cols}, etl_time)
                     SELECT {cols}, now()
                       FROM (SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY ({uniq_col}) ORDER BY updated_ts DESC) = 1 is_last_change
                               FROM stage.dlt_{tbl_name}) x
                      WHERE is_last_change;\n
                      COMMIT;
                     """.format(tbl_name=raw_tbl_name, uniq_col=','.join(tbl_aux['unique_cols']), cols=columns)
        self.cursor.execute(sql_script)
        for sfile in status_fiels:
            sfile = os.path.join(tbl_extraction_dir, 'status', sfile)
            os.rename(sfile, sfile.replace('ready.', 'done.'))

    def manage_stage_tables(self, extraction_dir, table_to_stage):
        "creates dlt and stg table. truncates delta tbl "
        raw_tbl_name = table_to_stage.split('.')[1]
        for prefix in ('dlt_', 'stg_'):
            sql_script = """SELECT EXISTS (SELECT 1 FROM  information_schema.tables 
                                            WHERE  table_schema = 'stage' AND table_name = '{}');
                         """.format(prefix+raw_tbl_name)
            self.cursor.execute(sql_script)
            exists = self.cursor.fetchone()[0]
            if not exists:
                with open(os.path.join(extraction_dir, table_to_stage.replace('.', '_'), 
                          'create_{}{}.sql'.format(prefix,raw_tbl_name))) as ddl_script:
                    self.cursor.execute(ddl_script.read())
            elif exists and prefix=='dlt_':
                self.cursor.execute('TRUNCATE TABLE stage.{}{};'.format(prefix,raw_tbl_name))

    def get_status_files(self, extraction_dir, table_to_stage):
        ready_files = []
        for sfile in os.listdir(os.path.join(extraction_dir, table_to_stage.replace('.', '_'), 'status')):
            if sfile.startswith('ready.'):
                ready_files.append(sfile)
        return sorted(ready_files, key=lambda x: x.split('.')[1])


def main(table_to_stage=None, extraction_dir=None, sql_script_path=None):
    etlh = EtlHandler()
    if (table_to_stage != None) and (extraction_dir != None):
        etlh.stage_data(extraction_dir, table_to_stage)
    elif sql_script_path != None:
        with open(sql_script_path, 'r') as input_sql:
            etlh.cursor.execute(input_sql.read())


if __name__ == '__main__':
    """
    example usage:
        -- stage table (moves inserted/updated rows to stage area)
        python etl_handler.py --table_to_stage='os.customers' --extraction_abs_path='/Users/stulski/Desktop/osobiste/fullstack_de/extraction'
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_to_stage', default=None, help='Table from OceanRecords db to be staged')
    parser.add_argument('--extraction_abs_path', default=None, help='Absolute path to folder with extraction data')
    parser.add_argument('--sql_script_path', default=None, help='Path to sql script')
    args = parser.parse_args()
    main(table_to_stage=args.table_to_stage,
         extraction_dir=args.extraction_abs_path,
         sql_script_path=args.sql_script_path)

