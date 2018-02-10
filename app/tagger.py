import os, sys, getopt
import unicodedata
from nltk.tag.stanford import StanfordPOSTagger as PTag
from nltk.stem import WordNetLemmatizer

reload(sys)
sys.setdefaultencoding("utf-8")

lmtzr = WordNetLemmatizer()
class tagger:

    def __init__(self):
        self.tagger = PTag("./POSTagger/models/english-bidirectional-distsim.tagger",
                           "./POSTagger/stanford-postagger.jar")

    def posTags(self,s):
        return self.tagger.tag(s.split())

    def tag(self,sentence):
        tags = self.posTags(sentence)
        print tags
        return tags


#t = tagger()
#t.tag("I am Siddarth")
