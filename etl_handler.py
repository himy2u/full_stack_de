"""
co potrzebuje...?:
    OK 1) extractor - skrypt ktory bierze jako konfiguracje tabele ze zrodla, zgarnia dane (ogarnia pobieranie tylko delty) i `wypluwa`
       plik (bede bazowal na csv dla prostoty) i definicje tabli stg
       ten skrypt `kontaktuje sie` ze zrodlem tylko i wylacznie
    2) etl_handler - ten skrypt sluzy do operacji na DWH: 
        OK + "data staging" (wez dane, stworz tabele jezeli trzeba, pilnuj wszelkich znacznikow czasu itp)
        + "spin dwh tables" (odpala ddl przygotowany do utowrzenia wrk i dwh tabel. proste ale zapobiega bleda ludzi)
        + "execute sql scrpt" (podstawowa funkcja do wszelkich operacji na dwh. odpalana przez airflow)
    3) airflow dags etc

jeszcze jakies inne notatki:
mam 4 zestawy tabel: delta, stg, wrk i dwh. nie chce powtarzac tego samego kodu DDL.... ponadto, dobrze by bylo zeby bylo to z automatu...

z drugiej strony, czlowiek musi zaprogramowac transform step.... wiec moze cos semi-automatycznego... ?

albo np czlowiek tez musi stworzyc wrk i dwh (stg moze byc z automatu bo mam definicje z systemu), to jest wsadzone do ETL jako 
opcjonalny krok? no ale i tak wtedy trzeba sie meczyc zeby trzymac "up-to-date" zmiany... innaczej to nie bedzie zsynchronizowane...

moze pozostac przy skrypcie, ktory musi byc odpalany recznie? i zawiera on poprostu 2 ddl z tabelami i spokoj... wychodzi na to samo
a dag ktorszy i nie mylacy...
"""
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
                          FROM '{}' \nHEADER DELIMITER ',' CSV NULL AS 'NULL'; 
                       """.format(raw_tbl_name, columns, data_file_path)
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


def main(table_to_stage=None, extraction_dir=None):
    etlh = EtlHandler()
    etlh.stage_data(extraction_dir, table_to_stage)

if __name__ == '__main__':
    """
    example usage:
        -- stage table (moves inserted/updated rows to stage area)
        python etl_handler.py --table_to_stage='os.customers' --extraction_abs_path='/Users/stulski/Desktop/osobiste/fullstack_de/extraction'
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_to_stage', default=None, help='Table from OceanRecords db to be staged')
    parser.add_argument('--extraction_abs_path', default=None, help='Absolute path to folder with extraction data')
    args = parser.parse_args()
    main(table_to_stage=args.table_to_stage,
         extraction_dir=args.extraction_abs_path)

