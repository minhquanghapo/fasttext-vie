import json
from bs4 import BeautifulSoup
from datetime import datetime
from sklearn.externals import joblib
from dateutil.parser import parse
from pymongo import MongoClient
from pyvi import ViTokenizer, ViPosTagger
import time
start_time = time.time()



class get_stories:
    
    special_html_characters = {
        u'&quot;': u'\"',
        u'&amp;': u'&',
        u'&frasl;': u'/',
        u'&lt;': u'<',
        u'&gt;': u'>'
    }

    config = {
        'MONGO_HOST' : '210.245.115.39',
        'MONGO_PORT' : 27017,
        'MONGO_USER' : '',
        'MONGO_PASS' : '',

        'MONGO_DB' : 'dbMongo',

        'MONGO_COLLECTION_ARTICLES' : 'articles',
    }
    def __init__(self):
        self.contentId = self.load_contentId()
        self.ids = {}
        self.new_stories = []
        self.new_titles = []
        self.new_categories = []
        self.new_dates = []
        self.new_publisher = []


    def run(self, db):
        print "start get content from db  ...."
        collection = db.get_collection(config['MONGO_COLLECTION_ARTICLES'])
        documents = collection.find({u'contentId' : {u'$gt' : self.contentId}})
        del self.new_stories[:]
        del self.new_titles[:]
        del self.new_categories[:]
        del self.new_dates[:]
        del self.new_publisher[:]
        i = 0
        for doc in documents:
            date_obj = parse(doc[u'date'])
            publisher = doc[u'publisherName'].strip()
            if self.check_date(date_obj):
                continue
            title, story, category = self.get_content(doc)
            if story == u'' or title == u'':
                continue

            self.new_dates.append(date_obj)
            self.new_publisher.append(publisher)
            self.new_stories.append(story.strip())
            self.new_titles.append(title)
            self.new_categories.append(category.lower())
            if len(self.new_stories)%10000 == 0:
                print "get ", len(self.new_stories), " items"
            if len(self.new_stories) > 1000:
                break

        print('There are %d new stories' % len(self.new_stories))
        avg = 0
        count = 0
        for story in self.new_stories:
            count += len(story)
        print "AVG: ", count/len(self.new_stories)
        self.save_contentId()


    def check_date(self, date_obj):
        # datetime_obj = date_obj.date()
        # now = datetime.now()
        # diff = now.date() - datetime_obj
        # if diff.days != 0:
        #     return True
        return False


    def get_content(self, doc):
        doc_id = doc[u'_id']
        if self.is_exist(doc_id):
            return u'', u'', u''
        contentId = doc[u'contentId']
        title = doc[u'title'].strip()
        if title != u'':
            title = u' == '.join([unicode(contentId), title])
            # print(title)
        else:
            return u'', u'', u''
        title = self.normalize_speical_html_chars(title)

        tags = map(lambda x: x.strip(), json.loads(doc[u'tags'], encoding='utf-8'))
        # tags += tags + tags # duplicate tags to emphasize important words
        tags = u'[tags] : ' + u' , '.join(tags)
        tags = self.normalize_speical_html_chars(tags)

        description = doc[u'description'].strip()
        description = self.normalize_speical_html_chars(description)

        raw_body = json.loads(doc[u'body'], encoding='utf-8')
        body = self.get_body(raw_body)
        body = self.normalize_speical_html_chars(body)

        story = u'\n'.join([title, description, body, tags])

        if contentId > self.contentId:
            self.contentId = contentId

        category = doc[u'parentCategoryName'].strip().lower()

        return title, story, category


    def get_body(self, raw_body):
        clean_body = []
        for content in raw_body:
            try:
                if content[u'type'] != u'text':
                    continue
                clean_content = BeautifulSoup(content[u'content']).text.strip()
                clean_body.append(clean_content)
            except:
                continue
        return u'\n'.join(clean_body)


    def is_exist(self, doc_id):
        try:
            _ = self.ids[doc_id]
            return True
        except:
            self.ids.update({doc_id : True})
            return False


    def clear(self):
        del self.new_stories[:]
        del self.new_titles[:]
        del self.new_categories[:]
        del self.new_dates[:]
        del self.new_publisher[:]
        self.ids.clear()


    def save_contentId(self):
        joblib.dump(self.contentId, 'contentId.pkl')


    def load_contentId(self):
        try:
            contentId = joblib.load('contentId.pkl')
            return contentId
        except: return 0


    def normalize_speical_html_chars(self, text):
        for code, char in get_stories.special_html_characters.items():
            text = text.replace(code, char)
        return text

    @staticmethod
    def connect2mongo(host, port, user, pwd, db_name):
        connection = MongoClient(host, port)
        db = connection[db_name]
        if user != '' and pwd != '':
            db.authenticate(user, pwd)
        return connection, db


if __name__ == '__main__':
    import io
    from regex import regex
    from tokenizer.tokenizer import Tokenizer

    config = get_stories.config
    connection, db = get_stories.connect2mongo(config['MONGO_HOST'], config['MONGO_PORT'],
                                         config['MONGO_USER'], config['MONGO_PASS'],
                                         config['MONGO_DB'])

    stories = get_stories()
    stories.run(db)
    r = regex()
    # Prepare Data
    with io.open('vie.txt', 'w+', encoding="utf-8") as f:
        for story in stories.new_stories:
            reg_text = r.run(story)
            token_text = ViTokenizer.tokenize(reg_text)
            f.write(token_text.lower())
            f.write(u'\n')
        print "Save data success into vie.txt"

    elapsed_time = time.time() - start_time
    print "Total_Time for Excute: ", elapsed_time

    connection.close()