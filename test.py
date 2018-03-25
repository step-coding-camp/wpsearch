import sys

all_ok = True

try:
    import natto
    mecab = natto.MeCab()
    mecab.parse("お元気でお過ごしですか")
    print("natto-py OK")
except ModuleNotFoundError:
    print("natto-pyモジュールが見つかりませんでした")
    all_ok = False
    print("モジュールサーチパス", sys.path)
except :
    print("Unexpected error:", sys.exc_info()[0])
    all_ok = False

try:
    import sqlite3
    db = sqlite3.connect(':memory:')
    cursor = db.cursor()
    cursor.execute('CREATE TABLE foo(id INTEGER PRIMARY KEY, name TEXT)')
    db.commit()
    db.close()
    print("sqlite3 OK")
except ModuleNotFoundError:
    print("sqlite3モジュールが見つかりませんでした")
    all_ok = False
    print("モジュールサーチパス", sys.path)
except :
    print("Unexpected error:", sys.exc_info()[0])
    all_ok = False

try:
    import bottle
    print("bottle OK")
except ModuleNotFoundError:
    print("bottleモジュールが見つかりませんでした")
    all_ok = False
    print("モジュールサーチパス", sys.path)
except :
    print("Unexpected error:", sys.exc_info()[0])
    all_ok = False

try:
    import tqdm
    print("tqdm OK")
except ModuleNotFoundError:
    print("tqdmモジュールが見つかりませんでした")
    all_ok = False
    print("モジュールサーチパス", sys.path)
except :
    print("Unexpected error:", sys.exc_info()[0])
    all_ok = False

if all_ok:
    print("事前準備チェック成功")
else:
    print("事前準備チェック失敗")

sys.exit(0 if all_ok else 1)
