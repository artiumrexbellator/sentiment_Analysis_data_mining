from facebook_scraper import get_posts
import csv
import os
from transformers import pipeline
import json
"""DistilBERT base uncased finetuned SST-2
This model is a fine-tune checkpoint of DistilBERT-base-uncased,
 fine-tuned on SST-2. 
 This model reaches an accuracy of 91.3 on the dev set 
 (for comparison, Bert bert-base-uncased version reaches an accuracy of 92.7). """

sentiment_pipeline = pipeline("sentiment-analysis")
topic = 'volkswagen'
nb_comments = 10


#write header of files function
def writeHeader(csvFile, fieldnames):
    dw = csv.DictWriter(csvFile, delimiter=',', fieldnames=fieldnames)
    dw.writeheader()


#sentiments analysis function with the return of sentiment label (positive,negative,neural)
#  + score for each post and comments
def sentiments(post, comments):
    post_result = sentiment_pipeline([post], batch_size=32)
    #the algorithm only supports 512 words,
    # i had to join all the text and then take first {nb_comments} comments,otherwise you can implement
    #your proper tokenizer that shuffles data
    data = ' '.join([x['comment_text'] for x in comments[:nb_comments]])
    comments_result = sentiment_pipeline([' '.join(data)],
                                         batch_size=32,
                                         num_workers=3)
    return [
        post_result[0]['label'], post_result[0]['score'],
        comments_result[0]['label'], comments_result[0]['score']
    ]


#saving topic as a json file
with open('topic.json', 'w', encoding='utf-8') as f:
    json.dump({"topic": topic}, f, ensure_ascii=False, indent=4)

#writing headers of csv files
current_path = os.path.abspath(os.getcwd())
posts_csv = open(current_path + '/posts.csv', 'w')
comments_csv = open(current_path + '/comments.csv', 'w')
replies_csv = open(current_path + '/replies.csv', 'w')

writeHeader(posts_csv, [
    "post_id", 'available', 'topic', 'comments', 'text', 'post_url', 'haha',
    'like', 'love', 'sorry', 'wow', 'shares', 'time', 'post_sen_label',
    'post_sen_index', 'comments_sen_label', 'comments_sen_index'
])
writeHeader(comments_csv,
            ['post_id', 'comment_id', 'commenter', 'text', 'time'])
writeHeader(replies_csv,
            ['comment_id', 'reply_id', 'commenter', 'text', 'time'])

posts_writer = csv.writer(posts_csv)
comments_writer = csv.writer(comments_csv)
replies_writer = csv.writer(replies_csv)

#collect posts with nested comments and replies of comments with the specefication of pages number
# + cookies for reactions over posts

for post in get_posts(topic,
                      pages=2,
                      options={
                          "comments": True,
                          "extra_info": True
                      },
                      cookies=current_path + '/facebook.com_cookies.txt'):
    sens = sentiments(post['text'], post['comments_full'])
    if (post['reactions'] is None):
        posts_writer.writerow([
            post['post_id'], post['available'], topic, post['comments'],
            post['text'], post['post_url'], 0, post['likes'], 0, 0, 0,
            post['shares'], post['time'], sens[0], sens[1], sens[2], sens[3]
        ])
        for comment in post['comments_full'][:nb_comments]:
            comments_writer.writerow([
                post["post_id"], comment["comment_id"],
                comment["commenter_name"], comment["comment_text"],
                comment["comment_time"]
            ])
            for reply in comment["replies"][:nb_comments]:
                replies_writer.writerow([
                    comment["comment_id"], reply["comment_id"],
                    reply["commenter_name"], reply["comment_text"],
                    reply["comment_time"]
                ])
    else:
        posts_writer.writerow([
            post['post_id'], post['available'], topic, post['comments'],
            post['text'], post['post_url'], post['reactions']['haha'],
            post['reactions']['like'], post['reactions']['love'],
            post['reactions']['sorry'], post['reactions']['wow'],
            post['shares'], post['time'], sens[0], sens[1], sens[2], sens[3]
        ])
        for comment in post['comments_full'][:nb_comments]:
            comments_writer.writerow([
                post["post_id"], comment["comment_id"],
                comment["commenter_name"], comment["comment_text"],
                comment["comment_time"]
            ])
            for reply in comment["replies"][:nb_comments]:
                replies_writer.writerow([
                    comment["comment_id"], reply["comment_id"],
                    reply["commenter_name"], reply["comment_text"],
                    reply["comment_time"]
                ])

posts_csv.close()
comments_csv.close()
replies_csv.close()
