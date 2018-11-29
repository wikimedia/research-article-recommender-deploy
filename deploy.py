#!/usr/bin/python3

import argparse
import json
import mysql.connector


def get_cmd_options():
    parser = argparse.ArgumentParser(
        description='Deploys recommendations to MySQL.')
    parser.add_argument('--config', help='configuration JSON file')
    return parser.parse_args()


def get_lang_id(cursor, lang):
    cursor.execute(
        "SELECT id FROM language_new WHERE code='%s' LIMIT 1;" % (lang))
    try:
        return cursor.fetchone()[0]
    except TypeError:
        print("No such language: %s" % lang)
        return None


def load_languages(cursor, tsv_file):
    """Load languages into the database"""
    load_data = "LOAD DATA LOCAL INFILE '%s' INTO TABLE language_new "\
        "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "\
        "(code) "\
        "SET id=NULL" %\
        (tsv_file)
    cursor.execute(load_data)


def load_scores(cursor, tsv_file, source_id, target_id):
    """Load recommendation scores into the database"""
    load_data = "LOAD DATA LOCAL INFILE '%s' "\
        "INTO TABLE article_recommendation_new "\
        "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "\
        "IGNORE 1 LINES "\
        "(wikidata_id, score) "\
        "SET id=NULL, source_id=%d, target_id=%d;" %\
        (tsv_file, source_id, target_id)
    cursor.execute(load_data)


def get_mysql_credentials(file):
    with open(file, 'r') as infile:
        return infile.readline(), infile.readline()


def create_mysql_tables(cursor, schema_file):
    with open(schema_file, 'r') as infile:
        cursor.execute(infile)


def replace_mysq_tables(cursor):
    """Backup old tables, use new ones"""
    sql = """
        DROP TABLE IF EXISTS article_recommendation_old, language_old;
        RENAME article_recommendation TO article_recommendation_old,
            language TO language_old,
            article_recommendation_new TO article_recommendation,
            language_new TO language;
        """
    cursor.execute(sql)


def main():
    options = get_cmd_options()
    with open(options.config, 'r') as config_file:
        config = json.load(config_file)

    config.mysql.user, config.mysql.password = get_mysql_credentials(
        config.mysql.credentials_file)

    ctx = mysql.connector.connect(
        host=config.mysql.host,
        port=config.mysql.port,
        user=config.mysql.user,
        passwd=config.mysql.password,
        database=config.mysql.database)
    cursor = ctx.cursor()

    create_mysql_tables(cursor, config.mysql.schema_file)
    load_languages(cursor, config.mysql.languages_file)

    # load scores
    for source, targets in config.predictions.items():
        source_id = get_lang_id(cursor, options.source)
        for target in targets:
            tsv_file = '%s/%s-%s.tsv' % (
                config.predictions_dir, source, target)
            target_id = get_lang_id(cursor, options.target)
            load_scores(cursor, tsv_file, source_id, target_id)
    ctx.commit()

    replace_mysq_tables(cursor)

    cursor.close()
    ctx.close()


if __name__ == '__main__':
    main()
