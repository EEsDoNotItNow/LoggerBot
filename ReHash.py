#!/usr/bin/env python


from PIL import Image
import imagehash
import logging
import pathlib
import sqlite3
import time
import shutil
import hashlib

import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--rowid', metavar='N', type=int,
                    help='row id to start processing at, defaults to all rows')
parser.add_argument('--threshold', metavar='N', type=int, default=5,
                    help='All values must be <= to this value to trigger a match (Defaults to 5)')

args = parser.parse_args()

# create logger with 'spam_application'
logger = logging.getLogger('spam_application')
logger.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh = logging.FileHandler('spam.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter = logging.Formatter('{asctime} {levelname} {filename}:{funcName}:{lineno} {message}', style='{')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
# logger.addHandler(fh)
logger.addHandler(ch)

logger.info(args)

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def table_exists(cur, table_name):
    cmd = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    if cur.execute(cmd).fetchone():
        return True
    return False

def mark_removed(file_name):
    cur.execute("UPDATE images SET removed=1 WHERE file_name=?",(str(file_name),))
    conn.commit()
    logger.info(f"File removed: {str(file_name)}")
    # time.sleep(1)

def insert_skip(file_name1, file_name2):
    cur.execute("INSERT OR IGNORE INTO skips (file_name1, file_name2) VALUES (?,?)", 
        (str(file_name1), str(file_name2)))
    conn.commit()

def insert_md5(file_name, md5):
    cur.execute("UPDATE images SET md5=? WHERE file_name=?",
        (
            md5,
            file_name
            )
        )
    conn.commit()


conn = sqlite3.connect("duplicates.db")
conn.row_factory = dict_factory
cur = conn.cursor()

logger.info("Check to see if images exists.")
if not table_exists(cur, "images"):
    logger.info("Create images table")
    cmd = """
        CREATE TABLE images
        (
            file_name TEXT UNIQUE,
            height INT,
            width INT,
            size INT,
            ahash TEXT,
            phash TEXT,
            dhash TEXT,
            whash TEXT,
            md5 TEXT,
            removed BOOLEAN DEFAULT 0
        )
    """
    cur.execute(cmd)
    conn.commit()
else:
    logger.info("images exists, continue")

logger.info("Check to see if skips exists.")
if not table_exists(cur, "skips"):
    logger.info("Create skips table")
    cmd = """
        CREATE TABLE skips
        (
            file_name1 TEXT,
            file_name2 TEXT,
            UNIQUE(file_name1, file_name2)
        )
    """
    cur.execute(cmd)
    conn.commit()
else:
    logger.info("images exists, continue")
# Find images not in DB

for p in pathlib.Path('/home/pheonix/Pictures').glob("**/*"):
    if not p.is_file():
        continue

    file_name = str(p)
    data = cur.execute("SELECT * FROM images WHERE file_name=?", (file_name,)).fetchone()
    if data is not None:
        logger.debug(f"File {p} is already in DB, skip")
        continue
    try:
        I = Image.open(p)
    except OSError:
        # logger.debug("Not an image...")
        continue
    except:
        logger.exception("What happened?")
        exit()
    logger.info(str(p))
    # print(I.width, I.height, I.size,p.stat().st_size)
    try:        
        ahash = imagehash.average_hash(I)
        phash = imagehash.phash(I)
        dhash = imagehash.dhash(I)
        whash = imagehash.whash(I)
    except:
        logger.exception("What happened?")
        exit()
    height = I.height
    width = I.width
    size = p.stat().st_size

    md5_current = hashlib.md5(Image.open(p).tobytes()).hexdigest()

    ahash = str(ahash)
    phash = str(phash)
    dhash = str(dhash)
    whash = str(whash)  
    cmd = """
        INSERT INTO images 
        (
            file_name,
            height,
            width,
            size,
            ahash,
            phash,
            dhash,
            whash,
            md5
        ) VALUES (                
            :file_name,
            :height,
            :width,
            :size,
            :ahash,
            :phash,
            :dhash,
            :whash,
            :md5_current
        )
        """
    cur.execute(cmd, locals())
    conn.commit()
    logger.info(f"Saved {p}")

# Process images to check for duplicates
logger.info("Import completed, now checking for duplicates")
t_last = time.time()

if args.rowid is None:
    args.rowid = cur.execute('SELECT max(rowid) as rowid FROM images').fetchone()['rowid']

logger.info(args.rowid)

for idx, current_image in enumerate(cur.execute("SELECT *,rowid FROM images WHERE removed=0 AND rowid<=? ORDER BY rowid DESC",(args.rowid,) ).fetchall()):
    logger.info(f"Last loop took {time.time() - t_last:.3f}s")
    logger.info(current_image['rowid'])
    t_last = time.time()
    p_current = pathlib.Path(current_image['file_name'])
    if not p_current.exists():
        mark_removed(p_current)
        logger.warning(f"Removed a non-existent file: {p_current}")
        continue
    ahash = imagehash.hex_to_hash(current_image['ahash'])
    phash = imagehash.hex_to_hash(current_image['phash'])
    dhash = imagehash.hex_to_hash(current_image['dhash'])
    whash = imagehash.hex_to_hash(current_image['whash'])
    md5_current = hashlib.md5(Image.open(p_current).tobytes()).hexdigest()
    if current_image['md5'] == None:
        logger.info(f"Update MD5 for {current_image['file_name']}")
        insert_md5(current_image['file_name'], md5_current)
    db_changed = False
    while 1:
        if not p_current.is_file():
            logger.info("Looks like our file is gone, break")
            break
        total_images = cur.execute("SELECT count(*) FROM images WHERE removed=0").fetchone()
        logger.info(f"Currently on rowid {current_image['rowid']}/{total_images['count(*)']} ({idx/total_images['count(*)']:.3%}) with file name {p_current}")
        db_changed = False

        # Load Skips
        skips = cur.execute("SELECT * FROM skips WHERE file_name1=?", (str(p_current),) ).fetchall()

        for db_image in cur.execute("SELECT * FROM images WHERE removed=0 AND file_name != ? AND rowid<?", 
                (
                    str(p_current),
                    current_image['rowid']
                )
            ).fetchall():

            p_db = pathlib.Path(db_image['file_name'])

            found_skip = False
            for skip in skips:
                if str(p_db) == skip['file_name2']:
                    found_skip = True
                    logger.info(f"Skipping per DB, file {skip['file_name2']}")
            if found_skip:
                break

            if not p_db.exists():
                mark_removed(p_db)
                logger.warning(f"Removed a non-existent file: {p_db}")
                db_changed = True
                break
            db_ahash = imagehash.hex_to_hash(db_image['ahash'])
            db_phash = imagehash.hex_to_hash(db_image['phash'])
            db_dhash = imagehash.hex_to_hash(db_image['dhash'])
            db_whash = imagehash.hex_to_hash(db_image['whash'])

            if db_phash - phash <= args.threshold and \
               db_ahash - ahash <= args.threshold and \
               db_dhash - dhash <= args.threshold and \
               db_whash - whash <= args.threshold:
                md5_db = hashlib.md5(Image.open(db_image['file_name']).tobytes()).hexdigest()
                logger.warning("Hashes match, show the images and quit!")
                logger.info("A:")
                logger.info(f"  File Name: {p_current} ")
                logger.info(f"       Size: {p_current.stat().st_size} ")
                logger.info(f"       MD5: {md5_current} ")
                logger.info("B:")
                logger.info(f"  File Name: {p_db} ")
                logger.info(f"       Size: {p_db.stat().st_size} ")
                logger.info(f"       MD5: {md5_db} ")
                logger.info(f"ahash: {db_ahash - ahash}")
                logger.info(f"phash: {db_phash - phash}")
                logger.info(f"dhash: {db_dhash - dhash}")
                logger.info(f"whash: {db_whash - whash}")
                logger.info(f"A to keep {p_current}")
                logger.info(f"B to keep {db_image['file_name']}")
                logger.info("D to display")
                logger.info(f"S to skip")
                if p_current.stat().st_size < db_image['size']:
                    logger.info(f"Suggesting B, as it is larger ({db_image['size']} > {p_current.stat().st_size})")
                elif p_current.stat().st_size > db_image['size']:
                    logger.info(f"Suggesting A, as it is larger ({db_image['size']} < {p_current.stat().st_size})")
                while 1:
                    if md5_current == md5_db:
                        logger.info("md5 is identical! Keeping A, moving B")
                        choice = 'a'
                    else:
                        choice = input("> ").lower()

                    if choice == "s":
                        insert_skip(p_current, p_db)
                        break

                    if choice == "d":
                        I1 = Image.open(p_current)
                        I2 = Image.open(db_image['file_name'])
                        I1.show()
                        I2.show()
                        continue

                    elif choice == "a":
                        file_to_move = pathlib.Path(db_image['file_name'])

                    elif choice == "b":
                        file_to_move = p_current

                    # TODO: This really should be a config value in the script
                    file_dest = pathlib.Path("/home/pheonix/Duplicates/",file_to_move.name)
                    while pathlib.Path(file_dest).is_file():
                        file_dest = pathlib.Path(file_dest.parent, file_dest.stem + "_copy" + file_dest.suffix)
                        logger.info(f"Had to rename file to {file_dest}")
                    shutil.move(str(file_to_move), str(file_dest))
                    mark_removed(file_to_move)
                    db_changed = True
                    break
            # Brself.time_stampeak to reset the for loop if we updated the db
            if db_changed == True:
                break
        # if we escaped the for loop WITHOUT a change, break from the while loop
        if db_changed == False:
            break
