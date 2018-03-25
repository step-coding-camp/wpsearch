import json
import gzip
import sys
import itertools
import datetime
import wp
import tqdm

class MutableWikipediaCollection(wp.WikipediaCollection):
    def __init__(self, filename):
        super().__init__(filename)
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            title TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            opening_text TEXT NOT NULL,
            auxiliary_text TEXT NOT NULL,
            categories TEXT NOT NULL,
            headings TEXT NOT NULL,
            wiki_text TEXT NOT NULL,
            popularity_score REAL NOT NULL,
            num_incoming_links INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS redirects (
            src TEXT PRIMARY KEY,
            dst TEXT NOT NULL
        );
        """)

    def insert_article_rows(self, rows):
        """Inserts rows into the articles table.
        
        Args:
            rows (List[(str, str, str, str, str, str, str, float, int)]): List of rows to insert.
        """
        self.db.executemany("INSERT INTO articles VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        self.db.commit()

    def insert_redirect_rows(self, rows):
        """Inserts rows into the redirects table.
        
        Args:
            rows (List[(str, str)]): List of rows to insert.
        """
        self.db.executemany("INSERT INTO redirects VALUES(?, ?)", rows)
        self.db.commit()

    def num_documents(self):
        """Returns the number of documents (i.e. WikipediaArticle).

        Override for Collection.
        
        Returns:
            int: The number of documents in the collection.
        """
        self._cached_num_documents = None
        return super().num_documents()


def parse_line_separated_json(filename):
    with gzip.open(filename) as f:
        for line in f:
            yield json.loads(line)

def parse_cirrus_dump(filename):
    objects = parse_line_separated_json(filename)
    while True:
        group = tuple(itertools.islice(objects, 2))
        if not group:
           return
        yield group

def main(argv):
    dump_filename = argv[1]
    db_filename = argv[2]
    collection = MutableWikipediaCollection(db_filename)
    with tqdm.tqdm(total=1097153, smoothing=0) as pbar:
        i = 0
        num_inserted_articles = 0
        num_nsfw_articles = 0
        num_unpopular_articles = 0
        BLOCK_SIZE = 1000
        rows = []
        redirect_rows = []
        for idx, o in parse_cirrus_dump(dump_filename):
            i += 1
            assert(idx['index']['_type'] == 'page')
            assert(o['namespace'] == 0)
            if 'Template:性的' in o['template']:
                num_nsfw_articles += 1
                continue
            if o.get('popularity_score', 0) < 0.000002:
                num_unpopular_articles += 1
                continue
            #if o['opening_text'] is None:
            #    print(o['title'])
            #print(o['opening_text'] is None)
            opening_text = o.get('opening_text', '')
            if opening_text is None:
                opening_text = ''
            rows.append((o['title'], o['text'], opening_text, json.dumps(o['auxiliary_text'], ensure_ascii=False), json.dumps(o.get('category', []), ensure_ascii=False), json.dumps(o.get('heading', []), ensure_ascii=False), o['source_text'], o.get('popularity_score', 0), o['incoming_links']))
            if 'redirect' in o:
                redirect_rows += [(src['title'], o['title']) for src in o['redirect'] if src['namespace'] == 0]
            pbar.update()
            num_inserted_articles += 1
            if len(rows) == BLOCK_SIZE:
                collection.insert_article_rows(rows)
                rows = []
                collection.insert_redirect_rows(redirect_rows)
                redirect_rows= []
        collection.insert_article_rows(rows)
        collection.insert_redirect_rows(redirect_rows)
        print ('total_pages', i)
        print ('num_inserted_articles', num_inserted_articles)
        print ('num_nsfw_articles', num_nsfw_articles)
        print ('num_unpopular_articles', num_unpopular_articles)
    collection.db.execute('DELETE FROM redirects WHERE dst IN (SELECT redirects.dst FROM redirects LEFT JOIN articles ON redirects.dst=articles.title WHERE articles.title IS NULL)')
    collection.db.isolation_level = None # https://bugs.python.org/issue28518
    collection.db.execute('VACUUM')
    collection.db.isolation_level = ''
    collection.db.commit()
    print ('done')
    print("total", i) # 1097153

main(sys.argv)
