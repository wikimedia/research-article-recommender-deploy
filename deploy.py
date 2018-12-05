#!/usr/bin/python3

import argparse
import mysql.connector


def get_cmd_options():
    parser = argparse.ArgumentParser(
        description='Deploys recommendations to MySQL.')
    parser.add_argument('action',
                        help='Action to perform.',
                        choices=['import_languages',
                                 'import_scores',
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
    parser.add_argument('--scores_file',
                        help='Scores file in TSV format. '
                        'Required when importin scores.')
    parser.add_argument('--source_language', help='Source language. '
                        'Required when importing scores.')
    parser.add_argument('--target_language', help='Target language. '
                        'Required when importing scores.')
    return parser.parse_args()


def get_lang_id(cursor, version, lang):
    cursor.execute(
        "SELECT id FROM language_%s WHERE code='%s' LIMIT 1;"
        % (version, lang))
    try:
        return cursor.fetchone()[0]
    except TypeError:
        print("No such language: %s" % lang)
        return None


def insert_scores_to_table(cursor, version, tsv_file, source_id, target_id):
    """Load recommendation scores into the database"""
    load_data = "LOAD DATA LOCAL INFILE '%s' "\
        "INTO TABLE article_recommendation_%s "\
        "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "\
        "IGNORE 1 LINES "\
        "(wikidata_id, score) "\
        "SET id=NULL, source_id=%d, target_id=%d;" %\
        (tsv_file, version, source_id, target_id)
    cursor.execute(load_data)


def get_mysql_password(file):
    with open(file, 'r') as infile:
        return infile.readline().strip()


def table_exists_p(cursor, database, table_name):
    sql = """
        SELECT *
        FROM information_schema.tables
        WHERE table_schema = '%s'
        AND table_name = '%s'
        LIMIT 1;
    """ % (database, table_name)
    cursor.execute(sql)
    return cursor.fetchone() is not None


def create_language_table(cursor, version):
    sql = """
        CREATE TABLE `language_%s` (
            `id` smallint(6) NOT NULL AUTO_INCREMENT,
            `code` varchar(8) NOT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `code` (`code`)
        ) ENGINE=InnoDB;
    """ % (version)
    cursor.execute(sql)


def insert_languages_to_table(cursor, version, tsv):
    sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE language_%s "\
        "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "\
        "(code) "\
        "SET id=NULL" %\
        (tsv, version)
    cursor.execute(sql)


def create_article_recommendation_table(cursor, version):
    sql = """
        CREATE TABLE `article_recommendation_%s` (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `wikidata_id` int(11) NOT NULL,
            `score` float NOT NULL,
            `source_id` smallint(6) NOT NULL,
            `target_id` smallint(6) NOT NULL,
            PRIMARY KEY (`id`),
            KEY `wikidata_id` (`wikidata_id`),
            KEY `source_id` (`source_id`),
            KEY `target_id` (`target_id`),
            CONSTRAINT `article_recommendation_ibfk_1`
                FOREIGN KEY (`source_id`) REFERENCES `language_%s` (`id`)
                ON DELETE CASCADE ON UPDATE CASCADE,
            CONSTRAINT `article_recommendation_ibfk_2`
                FOREIGN KEY (`target_id`) REFERENCES `language_%s` (`id`)
                ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB;
    """ % (version, version, version)
    cursor.execute(sql)


def import_languages(cursor, database, version, tsv):
    table_name = 'language_%s' % version

    if table_exists_p(cursor, database, table_name):
        print('Table %s already exists. Either drop it first '
              'or import a new version.' % table_name)
        exit(1)

    create_language_table(cursor, version)
    insert_languages_to_table(cursor, version, tsv)


def import_scores(cursor, database, version, tsv, source, target):
    table_name = 'article_recommendation_%s' % version

    if table_exists_p(cursor, database, table_name):
        print('Table %s already exists. '
              'Either drop it first or import a new version.' % table_name)
        exit(1)

    create_article_recommendation_table(cursor, version)
    source_id = get_lang_id(cursor, version, source)
    target_id = get_lang_id(cursor, version, target)
    if not source_id:
        print("Source language doesn't exist in the database.")
        exit()
    if not target_id:
        print("Target language doesn't exist in the database.")
    insert_scores_to_table(cursor, version, tsv, source_id, target_id)


def create_views(cursor, version):
    sql = """
        CREATE OR REPLACE VIEW language
          AS SELECT * FROM language_%s;
        CREATE OR REPLACE VIEW article_recommendation
          AS SELECT * FROM article_recommendation_%s;
    """ % (version, version)
    cursor.execute(sql, multi=True)
    print('Created views for tables language_%s '
          'and article_recommendation_%s.' % (version, version))


def cleanup_old_data(cursor, version):
    sql = """
        DROP TABLE IF EXISTS language_%s;
        DROP TABLE IF EXISTS article_recommendation_%s;"
    """ % (version, version)
    cursor.execute(sql, multi=True)
    print('Dropped tables language_%s and article_recommendation_%s '
          'if they existed. Make sure to update your views if they '
          'depended on this table version.' % (version, version))


def main():
    options = get_cmd_options()
    # print(get_mysql_password(options.mysql_password_file))
    # exit()
    ctx = mysql.connector.connect(
        host=options.mysql_host,
        port=options.mysql_port,
        user=options.mysql_user,
        passwd=get_mysql_password(options.mysql_password_file),
        database=options.mysql_database)
    cursor = ctx.cursor()

    print("Starting ...")
    if 'import_languages' == options.action:
        if not options.language_file:
            print('The languages file is not supplied.')
            exit(1)
        import_languages(cursor, options.mysql_database,
                         options.version, options.language_file)
    elif 'import_scores' == options.action:
        if not options.source_language or not options.target_language or\
           not options.scores_file:
            print('Source and target language are required.')
            exit(1)
        import_scores(cursor, options.mysql_database,
                      options.version, options.scores_file,
                      options.source_language, options.target_language)
    elif 'create_views' == options.action:
        create_views(cursor, options.version)
    elif 'cleanup' == options.action:
        cleanup_old_data(cursor, options.version)

    ctx.commit()
    cursor.close()
    ctx.close()

    print("Done")


if __name__ == '__main__':
    main()
