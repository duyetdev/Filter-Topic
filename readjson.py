from __future__ import print_function

import os, shutil
import sys
import json
import re
from pprint import pprint
from operator import add
import subprocess
from pyspark import SparkContext

##########################################################333
# Variable 
basedir = os.path.dirname(__file__)
tmpdir  = basedir + '/tmp'
vnTaggerDirPath = basedir + '/vnTagger'

vnTaggerExecFile = vnTaggerDirPath + '/vnTagger.sh'

num_line_comment_of_each_tmp_file = 300

def tokenize(text):
    # print('--> Tokenize: ' + text.encode('UTF-8'))
      return re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)

def is_deny    (content):
    return False

def filter_comment(comment): 
    return True

def run(cmd):
   call = ["/bin/bash", "-c", cmd]
   ret = subprocess.call(call, stdout=None, stderr=None)

def clean_content_dir(dir_path):
    folder = dir_path
    print('----> Cleanning folder %s' % folder)
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception, e:
            print (e)
    print ('----> Finish!')

################################################################################3
# Main spark
if __name__ == "__main__":
    filepath = basedir + '/raw_data.json'

    sc = SparkContext(appName="PythonWordCount")

    with open(filepath) as data_file:    
        data = json.load(data_file)

    tmp = list()
    # Group post
    for row in data:
        content = row['content'].strip()
        tmp.append(content)

    all_post = sc.parallelize(tmp)
    tokenize_post = all_post \
        .flatMap(lambda x: tokenize(x)) \
        .map(lambda x: x) \
        .filter(lambda x: filter_comment(x) == True)
        #.reduceByKey(add)

    comment_collection = tokenize_post.collect()
    num_of_collection = tokenize_post.count()

    num_of_file = num_of_collection / num_line_comment_of_each_tmp_file + 1;

    print ('--------------------> Write %s dataset to %s local file ...' % (num_of_collection, num_of_file))
    
    print ('--------------------> Clean content of temp folder....')
    clean_content_dir(tmpdir + '/in')
    clean_content_dir(tmpdir + '/out')

    comment_data_files = [open(tmpdir + '/in/comment_data_file_%s.txt' % i, "a") for i in range(1, num_of_file + 1)]
    print(comment_data_files[0])

    i = 0
    file_id = 0
    for row in comment_collection:
        i+=1
        if i >= num_line_comment_of_each_tmp_file:
            file_id += 1
            i = 0
        comment_data_files[file_id].write(row.encode("UTF-8") + '\n')
    # comment_data_file.close()
    print('---------------------> Write %s line to %s file.' % (i, file_id))
    
    print('---------------------> Run tagger all comments')
    os.chdir(vnTaggerDirPath)
    
    for i in range(1, num_of_file):
        run("%s -i %s -o %s -u -p" % (vnTaggerExecFile, tmpdir + "/in/comment_data_file_%s.txt" % i, tmpdir + "/out/comment_data_file_%s_tagged.txt" % i))
    

    print('----------------------> all_post: ' + str(all_post.count()))
    print('----------------------> Open context 2nd...')
    
    sc.stop()
    sc2 = SparkContext(appName="DSS_Filter")

    files = [tmpdir + "/out/comment_data_file_%s_tagged.txt" % i for i in range(1, num_of_file)]
    comments_tagged_collections = sc2.textFile(','.join(files))
    comments_tagged_count = comments_tagged_collections.count()

    print('-----------------------> Number of tagger comment is %s' % comments_tagged_count)

    sc.stop()
    
    #for row in comment_collection:
        #print(row.encode("UTF-8") + '\n\n ------------ \n\n')

    '''
        if not is_deny(content):
            for item in tokenize(content):
                if item.strip():
                    group_post.append(item.strip())

    c = sc.parallelize(group_post)
    pprint(group_post)
    '''
    
    '''
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word.encode("UTF-8"), count))
    '''

    sc.stop()