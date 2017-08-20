import hashlib
import os
import psycopg2
import sys


MACRO_SYS_JOURNAL_PATH = os.path.join(os.environ['WEASYL_ROOT'], 'static/journal')


def get_hash_path(journal_id):
    id_hash = hashlib.sha1(str(journal_id).encode('utf-8')).hexdigest()
    hash_path = [id_hash[i:i + 2] for i in range(0, 11, 2)]

    return os.path.join(MACRO_SYS_JOURNAL_PATH, *hash_path, f'{journal_id}.txt')


def journal_file_exists(journal_id):
    return os.path.exists(get_hash_path(journal_id))


def main(connection_string):
    with psycopg2.connect(connection_string) as db, db.cursor() as cur:
        cur.execute('SELECT journalid FROM journal ORDER BY journalid')

        missing = 0
        total = 0

        for journal_id, in cur:
            if not journal_file_exists(journal_id):
                print(journal_id)
                missing += 1

            total += 1

        print(f'{missing} of {total} missing', file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: journal-check.py <connection_string>', file=sys.stderr)
        raise SystemExit(1)

    main(sys.argv[1])
