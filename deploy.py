#!/usr/bin/python3

import argparse
import mysql.connector
import os
from shutil import rmtree


MAX_CHUNK_SIZE = 50000 # Maximum number of rows in a chunk when importing normalized rank tsv.


def get_cmd_options():
    parser = argparse.ArgumentParser(
        description='Deploys recommendations to MySQL.')
    parser.add_argument('action',
                        help='Action to perform.',
                        choices=['import_languages',
                                 'import_normalized_ranks',
                                 'create_views',
                                 'cleanup'])
    parser.add_argument('version', help='MySQL table version.')
    parser.add_argument('mysql_host', help='MySQL host.')
    parser.add_argument('mysql_port', help='MySQL port.')
    parser.add_argument('mysql_database', help='MySQL database.')
    parser.add_argument('mysql_user', help='MySQL user.')
    parser.add_argument('mysql_password_file',
                        help='File where MySQL password is stored.')
    parser.add_argument('--language_file',
                        help='Languages file in TSV format. '
                        'Required when importing languages.')
    parser.add_argument('--normalized_ranks_file',
                        help='Normalized ranks file in TSV format. '
                        'Required when importing normalized ranks.')
    parser.add_argument('--source_language', help='Source language. '
                        'Required when importing normalized ranks.')
    parser.add_argument('--target_language', help='Target language. '
                        'Required when importing normalized ranks.')
    return parser.parse_args()


def get_lang_id(cursor, version, lang):
    sql = "SELECT id FROM language_{version} WHERE code='{lang}' LIMIT 1"
    data = {
        'version': version,
        'lang': lang
    }
    cursor.execute(sql.format(**data))
    try:
        return cursor.fetchone()[0]
    except TypeError:
        print("No such language: %s" % lang)
        return None


def chunk_name(chunk_number, dir_name):
    """Get the name of chunk file"""
    return os.path.join(dir_name, ('chunk-' + str(chunk_number) + '.tsv'))


def get_temp_directory_name(tsv):
    """Returns the temporary directory name."""
    return os.path.join('/tmp', tsv.replace('/', '-'))


def delete_directory_if_exists(dir_name):
    """Deletes the directory in case it exists."""
    if os.path.exists(dir_name):
        rmtree(dir_name)


def create_tsv_chunks(dir_name, tsv):
    """Creates chunks and returns the list of generated chunks"""
    chunks = []
    with open(tsv, 'r') as tsv_file:
        next(tsv_file)
        current_chunk_number = 0
        new_chunk_name = chunk_name(current_chunk_number, dir_name)
        chunks.append(new_chunk_name)
        with open(new_chunk_name, 'w') as chunk_file:
            for line_number, line in enumerate(tsv_file):
                chunk_number = (line_number + 1) // MAX_CHUNK_SIZE
                chunk_file.write(line)
                if chunk_number != current_chunk_number:
                    chunk_file.close()
                    current_chunk_number = chunk_number
                    new_chunk_name = chunk_name(current_chunk_number, dir_name)
                    chunks.append(new_chunk_name)
                    chunk_file = open(new_chunk_name, 'w')
            chunk_file.close()
    return chunks


def insert_chunk_to_table(tsv_file, version, source_id, target_id, cursor, context):
    """Load the chunk into the database"""
    sql = ("LOAD DATA LOCAL INFILE '{tsv}' "
            "INTO TABLE normalized_rank_{version} "
            "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "
            "(wikidata_id, normalized_rank) "
            "SET source_id={source_id}, target_id={target_id}")
    data = {
        'tsv': tsv_file,
        'version': version,
        'source_id': source_id,
        'target_id': target_id
    }
    try:
        cursor.execute(sql.format(**data))
    except Exception as e:
        print(e)
        context.rollback()
        exit(1)


def insert_normalized_ranks(cursor, context, version, tsv, source_id,
                            target_id):
    """
    Creates chunks of recommendation normalized ranks tsv file, inserts the
    chunks into the database and deletes the chunks.
    """
    dir_name = get_temp_directory_name(tsv)
    delete_directory_if_exists(dir_name)
    os.mkdir(dir_name)
    chunks = create_tsv_chunks(dir_name, tsv)
    for tsv_file in chunks:
        insert_chunk_to_table(tsv_file, version, source_id, target_id, cursor, context)
        os.remove(tsv_file)
    rmtree(dir_name)


def get_mysql_password(file):
    with open(file, 'r') as infile:
        return infile.readline().strip()


def table_exists_p(cursor, database, table):
    sql = ("SELECT * "
           "FROM information_schema.tables "
           "WHERE table_schema = '{database}' "
           "AND table_name = '{table}' "
           "LIMIT 1")
    data = {
        'database': database,
        'table': table
    }
    cursor.execute(sql.format(**data))
    return cursor.fetchone() is not None


def create_language_table(cursor, version):
    sql = ("CREATE TABLE `language_{version}` ( "
           "`id` smallint(6) NOT NULL AUTO_INCREMENT, "
           "`code` varchar(8) NOT NULL, "
           "PRIMARY KEY (`id`), "
           "UNIQUE KEY `code` (`code`) "
           ") ENGINE=InnoDB")
    data = {
        'version': version
    }
    cursor.execute(sql.format(**data))


def insert_languages_to_table(cursor, version, tsv):
    sql = ("LOAD DATA LOCAL INFILE '{tsv}' INTO TABLE language_{version} "
           "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "
           "(code) "
           "SET id=NULL")
    data = {
        'tsv': tsv,
        'version': version
    }
    cursor.execute(sql.format(**data))


def create_normalized_rank_table(cursor, version):
    sql = ("CREATE TABLE `normalized_rank_{version}` ( "
           "`wikidata_id` int(11) NOT NULL, "
           "`normalized_rank` float NOT NULL, "
           "`source_id` smallint(6) NOT NULL, "
           "`target_id` smallint(6) NOT NULL, "
           "KEY `wikidata_id` (`wikidata_id`), "
           "KEY `source_id` (`source_id`), "
           "KEY `target_id` (`target_id`), "
           "CONSTRAINT `normalized_rank_ibfk_1_{version}` "
           "FOREIGN KEY (`source_id`) REFERENCES `language_{version}` (`id`) "
           "ON DELETE CASCADE ON UPDATE CASCADE, "
           "CONSTRAINT `normalized_rank_ibfk_2_{version}` "
           "FOREIGN KEY (`target_id`) REFERENCES `language_{version}` (`id`) "
           "ON DELETE CASCADE ON UPDATE CASCADE "
           ") ENGINE=InnoDB")
    data = {
        'version': version
    }
    cursor.execute(sql.format(**data))


def import_languages(cursor, database, version, tsv):
    table_name = 'language_%s' % version

    if table_exists_p(cursor, database, table_name):
        print('Table %s already exists. Either drop it first '
              'or import a new version.' % table_name)
        exit(1)

    create_language_table(cursor, version)
    insert_languages_to_table(cursor, version, tsv)


def import_normalized_ranks(cursor, context, database, version, tsv, source, target):
    table_name = 'normalized_rank_%s' % version

    if not table_exists_p(cursor, database, table_name):
        create_normalized_rank_table(cursor, version)
        print('Created table %s.' % table_name)

    source_id = get_lang_id(cursor, version, source)
    target_id = get_lang_id(cursor, version, target)
    if not source_id:
        print("Source language doesn't exist in the database.")
        exit()
    if not target_id:
        print("Target language doesn't exist in the database.")
    insert_normalized_ranks(cursor, context, version, tsv,
                            source_id, target_id)


def create_views(cursor, version):
    cursor.execute("CREATE OR REPLACE VIEW language "
                   "AS SELECT * FROM language_%s" % version)
    print('Created view "language" for the table "language_%s".' % version)

    cursor.execute("CREATE OR REPLACE VIEW normalized_rank "
                   "AS SELECT * FROM normalized_rank_%s" % version)
    print('Created view "normalized_rank" for the table '
          '"normalized_rank_%s".' % version)


def cleanup_old_data(cursor, database, version):
    normalized_rank_table = 'normalized_rank_%s' % version
    language_table = 'language_%s' % version

    if table_exists_p(cursor, database, normalized_rank_table):
        cursor.execute("DROP TABLE %s;" % normalized_rank_table)
        print('Dropped table %s.' % normalized_rank_table)
    else:
        print('Table %s doesn\'t exist.' % normalized_rank_table)

    if table_exists_p(cursor, database, language_table):
        cursor.execute("DROP TABLE %s;" % language_table)
        print('Dropped table %s.' % language_table)
    else:
        print('Table %s doesn\'t exist.' % language_table)

    print('Make sure to update the views if they '
          'depended on dropped tables.')


def main():
    options = get_cmd_options()
    context = mysql.connector.connect(
        host=options.mysql_host,
        port=options.mysql_port,
        user=options.mysql_user,
        passwd=get_mysql_password(options.mysql_password_file),
        database=options.mysql_database,
        client_flags=[
            mysql.connector.constants.ClientFlag.LOCAL_FILES,
            mysql.connector.constants.ClientFlag.MULTI_STATEMENTS
        ])
        # autocommit=True)
    cursor = context.cursor()

    print("Starting ...")
    if 'import_languages' == options.action:
        if not options.language_file:
            print('The languages file is not supplied.')
            exit(1)
        import_languages(cursor, options.mysql_database,
                         options.version, options.language_file)
    elif 'import_normalized_ranks' == options.action:
        if not options.source_language or not options.target_language or\
           not options.normalized_ranks_file:
            print('Source and target language are required.')
            exit(1)
        import_normalized_ranks(cursor,
                                context,
                                options.mysql_database,
                                options.version,
                                options.normalized_ranks_file,
                                options.source_language,
                                options.target_language)
    elif 'create_views' == options.action:
        create_views(cursor, options.version)
    elif 'cleanup' == options.action:
        cleanup_old_data(cursor, options.mysql_database, options.version)

    context.commit()
    context.close()

    print("Done")


if __name__ == '__main__':
    main()
