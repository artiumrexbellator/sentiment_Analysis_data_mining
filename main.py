import scraper
import subprocess
import os

current_path = os.path.abspath(os.getcwd())

print(
    "\n\n\n-- Welcome to py scraper + etl process based on pdi jobs and transformations -- \n\n\n"
)

topic = input("please mention a topic --->")
nb_pages = input("please mention number of pages to scrap --->")
nb_comments = input(
    "please mention number of comments to treat for each post --->")
try:
    sc = scraper.Scraper(topic, int(nb_comments), int(nb_pages))
    sc.generate_data()
    print("-- successfully generated data --")
except:
    print("-- given inputs are false  --")
    quit()

kitchen = input(
    "\n\nplease provide full path dir to kitchen.sh with execute permission granted -->"
)

cmd = f"/kitchen.sh -file {current_path}/kettle/etl_posts.kjb -job etl_posts \
    -param:posts_path='{current_path}/posts.csv' \
    -param:comments_path='{current_path}/comments.csv' \
    -param:replies_path='{current_path}/replies.csv' \
    -param:topic_path='{current_path}/topic.json'"

print("\n\n please wait while running the job ")
out = subprocess.getstatusoutput(kitchen + cmd)

if out[0] == 0:
    print("The job is successfully run")

quit()