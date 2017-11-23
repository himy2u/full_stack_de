"""
Full-stack Data Engineer, Part 2 - ETL

That script will perfrom extract step for ETL process. Although both source system and DWH are actualy same database in this example,
it will treat them as they would be complete separate systems.

Script outputs to defined directory set of folders and files per each extracted table. It keeps track of `watermark`, so it gets only 
changed rows. Along with csv with data, it outputs some metadata, status file (used later for loading into stage area in DWH) and 
DDL for stage table with DWH.

Files structures after extraction looks like:
/extraction_dir
    /src_table
          /data
            src_table_<ts>.csv
          /metadata
            src_table_<ts>.josn
            last.json
          /status
            src_table_<ts>.ready
          create_stg_table.sql
          create_dlt_table.sql
          table.aux
"""
# built-in
import argparse
import csv
from datetime import datetime
import json
import os

# 3rd party
import psycopg2


class Extractor():
    def __init__(self, table=None, extraction_dir=None):
        self.src_tbl = table
        self.extraction_dir = extraction_dir
        self.tbl_extraction_dir = os.path.join(self.extraction_dir, self.src_tbl.replace('.', '_'))
        self.metadata_path = os.path.join(self.tbl_extraction_dir, 'metadata')
        self.data_path = os.path.join(self.tbl_extraction_dir, 'data')
        self.status_path = os.path.join(self.tbl_extraction_dir, 'status')
        self.last_meta_filename = 'last.json'
        self.conn = psycopg2.connect(dbname='oceanrecords', host='localhost', port=5432, user='os_admin', password='getme')
        self.cursor = self.conn.cursor()
        self.tbl_def = self.get_tbl_definition()
        self.from_watermark = self.get_previous_to_watermark()
        self.to_watermark = self.from_watermark # at beginning `from_watermark` and `to_watermark` is the same
        self.prepare_extraction_folders()

    def extract(self):
        colnames = [el['column_name'] for el in self.tbl_def]
        sql_script = "SELECT {} FROM {} WHERE updated_ts > '{}'".format(','.join(colnames), self.src_tbl, self.from_watermark)
        self.cursor.execute(sql_script)
        data_filename = self.src_tbl.replace('.', '_') + '_' + datetime.now().strftime("%Y-%m-%dT%H-%M-%S") + '.csv'
        no_lines = 0
        row = self.cursor.fetchone()
        if (row is None) or (row == []):
            print('No new data to extract for table: ', self.src_tbl)
            return
        # save data and update meta
        with open(os.path.join(self.data_path, data_filename), 'w') as output_csv:
            writer = csv.writer(output_csv)
            writer.writerow(colnames)
            while row is not None and row != []:
                writer.writerow(row)
                no_lines += 1
                # naively assumes that last column is always updated_ts
                self.to_watermark = max(self.to_watermark, row[-1].strftime("%Y-%m-%dT%H:%M:%S.%f"))
                row = self.cursor.fetchone()
        # save metadata
        metadata = {"table": self.src_tbl,
                    "from_watermark": self.from_watermark,
                    "to_watermark": self.to_watermark,
                    "no_of_rows": no_lines,
                    "filename": data_filename}
        for file_name in (self.last_meta_filename, data_filename.replace('csv', 'json')):
            with open(os.path.join(self.metadata_path, file_name), 'w') as output_meta:
                json.dump(metadata, output_meta)
        # save empty status file
        open(os.path.join(self.status_path, 'ready.'+file_name.replace('.json', '')), 'w').close() 
        # save table auxiliary info
        with open(os.path.join(self.tbl_extraction_dir, self.src_tbl.replace('.', '_')+'.aux'), 'w') as output_aux:
            json.dump({"table": self.src_tbl, "unique_cols":self.get_pkeys(), "columns":colnames}, output_aux)
        self.output_tbl_ddl()
        print('Extracted {} to {}. Got: no_lines: {}, from_watermark: {}, to_watermark: {}'.format(self.src_tbl, data_filename, no_lines, 
                                                                                                   self.from_watermark, self.to_watermark))

    def get_tbl_definition(self):
        schema, table = self.src_tbl.split('.')
        cols = ['column_name', 'udt_name', 'character_maximum_length', 'numeric_precision', 'numeric_scale']
        sql_script = """
                     SELECT {}
                       FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE table_schema = '{}' AND table_name = '{}';
                     """.format(','.join(cols), schema, table)
        self.cursor.execute(sql_script)
        return [{k:v for k,v in el} for el in [zip(cols, row) for row in self.cursor.fetchall()]]

    def prepare_extraction_folders(self):
        for folder in (self.extraction_dir, self.tbl_extraction_dir, self.metadata_path, self.data_path, self.status_path):
            if not os.path.exists(folder):
                os.mkdir(folder)

    def get_previous_to_watermark(self):
        if os.path.isfile(os.path.join(self.metadata_path, self.last_meta_filename)):
            with open(os.path.join(self.metadata_path, self.last_meta_filename), 'r') as input_file:
                return json.loads(input_file.read())['to_watermark']
        return '1990-01-01 00:00:00'

    def get_pkeys(self):
        sql_script = """SELECT a.attname AS pkey
                          FROM   pg_index i
                          JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                                AND a.attnum = ANY(i.indkey)
                         WHERE  i.indrelid = '{}'::regclass
                                AND i.indisprimary;
                     """.format(self.src_tbl)
        self.cursor.execute(sql_script)
        return [row[0] for row in self.cursor.fetchall()]

    def output_tbl_ddl(self):
        for ddl_prefix in ('stg', 'dlt'):
            tbl = self.src_tbl.split('.')[1]
            ddl = 'CREATE TABLE stage.{}_{} (\n'.format(ddl_prefix, tbl)
            for idx, col in enumerate(self.tbl_def):
                if not col['udt_name'] == 'numeric':
                    ddl += '  {}      {}'.format(col['column_name'], col['udt_name'])
                else:
                    ddl += '  {}      {}({},{})'.format(col['column_name'], col['udt_name'], 
                                                        col['numeric_precision'], col['numeric_scale'])
                if idx<(len(self.tbl_def)-1):
                    ddl += ',\n'
                else:
                    if ddl_prefix == 'stg':
                        # add here also etl_time column
                        ddl += ',\n  etl_time      timestamp'
                    ddl += '\n);'
                ddl_filename = os.path.join(self.tbl_extraction_dir, 'create_{}_{}.sql'.format(ddl_prefix, tbl))
                with open(ddl_filename, 'w') as output_ddl:
                    output_ddl.write(ddl)


if __name__ == '__main__':
    """
    example usage: python extractor.py --table os.products
    NOTE: table have to contain schema name before table name
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', help='Table from OceanRecords db to be extracted')
    parser.add_argument('--extraction_dir', default='extraction', help='The root folder for extraction data/metadata')
    args = parser.parse_args()
    extractor = Extractor(table=args.table, extraction_dir=args.extraction_dir)
    extractor.extract()
