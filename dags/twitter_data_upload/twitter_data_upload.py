# Twitter API
from constants import consumer_key, consumer_secret, access_token, access_token_secret
import tweepy

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Database
from datetime import datetime
import psycopg2
from psycopg2_utilities import run_query
from constants import db, db_user, db_pw, db_host

dsn = "dbname={db} user={db_user} password={db_pw} host={db_host} port=5432".format(db=db, db_user=db_user, db_pw=db_pw, db_host=db_host)

# Core data upload loop
search_terms = ['#bitcoin', '#ethereum', '#litecoin', '#xrp', '#eos']

for search_term in search_terms:
	# Cursor method gets most recent results from Twitter API.
	c = tweepy.Cursor(api.search, q=search_term)
	# TODO: Speed up this operation.
	tuples = [(t.id,
		t.user.id,
		t.created_at.strftime('%Y-%m-%d %H:%M:%S'),
		search_term,
		t.text.replace("'", '')) for t in c.items(1000)]
	tuple_string = ",".join("(%s,%s,'%s','%s','%s')" % tup for tup in tuples)

	upsert_query = '''
	insert into tweets (tweet_id, user_id, created_at, search_term, text)
	values {tuple_string}
	on conflict (tweet_id) do update
	set user_id = excluded.user_id,
			created_at = excluded.created_at,
			search_term = excluded.search_term,
			text = excluded.text;
	'''.format(tuple_string=tuple_string)

	result = run_query(q=upsert_query, dsn=dsn)