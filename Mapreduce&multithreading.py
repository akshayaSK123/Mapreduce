#Reading file
from google.colab import files
uploaded = files.upload()

#Code for mapper reducer funtionality in python

class split_data:
  def __init__(self, part1, part2):
    self.part1 = part1
    self.part2 = part2

#Function for data cleaning
def data_clean(filename,out_queue):
  file = open(filename, 'rt')
  text = file.read()
  file.close()
  tokens = text.splitlines()
  # converting everything to lower case
  tokens_lower = [w.lower() for w in tokens]
  tokens_nod=  [re.sub("'s", "", word) for word in tokens_lower]
  # removing punctuation from text
  table=str.maketrans(string.punctuation, ' '*len(string.punctuation))
  #table = str.maketrans('', '', string.punctuation)
  stripped = [w.translate(table) for w in tokens_nod]
  # removing all non-alphabetic data from text
  words = [re.sub('\d', '', word) for word in stripped]
  out_queue.put(words)

#Function for splitting the data  
def data_split(words,out_queue):
  #Splitting first 5000 lines 
  part1=words[0:5000]
  #Splitting remaining lines 
  part2=words[5000:13030]
  result = split_data(part1, part2)
  out_queue.put(result)

#Function for mapper logic  
def mapper(mapp,out_queue):
  total_words=[]
  for line in mapp:
    words = word_tokenize(line)
    total_words.extend(words)
  d={}
  for word in total_words:
    d[word]=d.get(word,0)+1 ## If the value exists in dictionary then return value else 0
  a=[]
  for key,value in d.items(): ##items list the list of tuples in the dictionary
    a.append((key,value))
  #Writing result to queue  
  out_queue.put(a)

#Function to sort and shuffle
def sort (a,b,out_queue):
  #Combining the data
  c=a+b
  #Sorting in ascending order
  c.sort(reverse=False)
  #Writing result to queue  
  out_queue.put(c)

#Function to partition the data from a:m and n:z
def partition(sortresult,out_queue):
  redinp1=[]
  redinp2=[]
  #Partitioning the data based on first character
  for i in sortresult:
    if(i[0][0]) in ['a','b','c','d','e','f','g','h','i','j','k','l','m']:
      redinp1.append((i[0],i[1]))
    else:
      redinp2.append((i[0],i[1]))
  result = split_data(redinp1, redinp2)
  #Writing result to queue  
  out_queue.put(result)

#Reducer function
def reducer (inp1,out_queue):
  red1 = []
  reducer1 = pd.DataFrame(inp1,columns=["Word","Frequency"])
  #Combining the count
  red1 = pd.pivot_table(reducer1, index=["Word"],aggfunc={"Frequency":np.sum})
  #Writing result to queue  
  out_queue.put(red1)

#Main function that combines all the results
def fin(red1,red2,out_queue):
  final = pd.concat([red1,red2])
  #Writing result to queue  
  out_queue.put(final)


import nltk
nltk.download('punkt')
import string
import re
from nltk.stem import PorterStemmer 
from nltk.tokenize import line_tokenize
from nltk.tokenize import word_tokenize
nltk.download('stopwords')
from nltk.corpus import stopwords
import threading
from queue import *
import pandas as pd
import numpy as np
filename = 'Pride_and_Prejudice.txt'
my_queue = Queue()
my_queue1 = Queue()
my_queue2 = Queue()
my_queue3 = Queue()
my_queue4 = Queue()
my_queue5 = Queue()
my_queue6 = Queue()
my_queue7 = Queue()
my_queue8 = Queue()
#Thread for data cleaning function
thread1 = threading.Thread(data_clean(filename, my_queue))
thread1.start()
thread1.join()
data_clean_result = my_queue.get()
#Thread for data splitting function
thread2 = threading.Thread(data_split(data_clean_result, my_queue1))
thread2.start()
thread2.join()
data_split_result = my_queue1.get()
#Thread for mapper function function for the first 5000 lines
thread3 = threading.Thread(target=mapper(data_split_result.part1, my_queue2))
#Thread for mapper function function for the rest of the text
thread4 = threading.Thread(mapper(data_split_result.part2, my_queue3))
thread3.start()
thread4.start()
thread3.join()
thread4.join()
mapper_result_1 = my_queue2.get()
mapper_result_2 = my_queue3.get()
#Thread for sort and shuffle function
thread5 = threading.Thread(sort(mapper_result_1,mapper_result_2, my_queue4))
thread5.start()
thread5.join()
sorter_result = my_queue4.get()
#Thread for partition function to split data starting from a-m and n-z
thread6 = threading.Thread(partition(sorter_result, my_queue5))
thread6.start()
thread6.join()
partition_result = my_queue5.get()
#Thread for reducer function for words starting from a-m
thread7 = threading.Thread(reducer(partition_result.part1, my_queue6))
#Thread for reducer function for words starting from n-z
thread8 = threading.Thread(reducer(partition_result.part2, my_queue7))
#Starting both reducer function threads
thread7.start()
thread8.start()
thread7.join()
thread8.join()
reducerresult1 = my_queue6.get()
reducerresult2 = my_queue7.get()
#Thread for main function to combine all the results
thread9 = threading.Thread(fin(reducerresult1,reducerresult2, my_queue8))
thread9.start()
thread9.join()
finalresult = my_queue8.get()
#Writing result to file
finalresult.to_csv('Final_result.csv')