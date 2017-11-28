import json, mysql, qanda, answerEntity_impl, questionEntity_impl, userEntity_impl
from pymongo import MongoClient

class QandA_Impl(qanda.QandA):
    #file = 'qanda.db'
    #conn = sqlite3.connect('qanda.db', isolation_level=None)
    #cursor = conn.cursor()


    def __init__(self):
        self.connect()
        self.userEntity_impl = userEntity_impl.UserEntity_Impl(self.db) #pass database here
        self.answerEntity_impl = answerEntity_impl.AnswerEntity_Impl(self.db)
        self.questionEntity_impl = questionEntity_impl.QuestionEntity_Impl(self.db)

    #LOOK INTO THIS
#    def teardown(self):
#        self.conn.close()

    def initialize( self ):
      """make sure database is empty by deleting all existing rows"""
      #self.cursor.execute('PRAGMA foreign_keys=ON')
      self.userEntity_impl.initialize()
      self.answerEntity_impl.initialize()
      self.questionEntity_impl.initialize()
      return

    def user_entity( self ):
      """return an object that implements UserEntity"""
      return self.userEntity_impl

    def question_entity( self ):
      """return an object that implements QuestionEntity"""
      return self.questionEntity_impl

    def answer_entity( self ):
      """return an object that implements AnswerEntity"""
      return self.answerEntity_impl

    def connect(self):
        #client = MongoClient(port=27017)
        self.db = MongoClient().qanda
        #self.conn = mysql.connector.connect(user='dtradke', password='goldrush',
        #                          host='127.0.0.1',
        #                          database='qanda')
        #self.conn.autocommit = True
        #self.cursor = self.conn.cursor()
