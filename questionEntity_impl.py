import json, qanda, uuid, pprint
from pymongo import MongoClient

def order(self):
    return self.score

class QuestionEntity_Impl(qanda.QuestionEntity):

    def __init__(self, db):
        self.db = db

    def initialize( self ):
      self.db.question.delete_many({"_id": {"$exists": True}})
      return

    def get( self, id ):
      """return object with matching id"""
      """KeyError exception should be thrown if id not found"""
      if id is not '0':
          for question in list(self.db.question.find({"_id": id})):
            pprint.pprint(question)
            user_obj = qanda.Question(question["_id"],question["body"])
      else:
          raise KeyError
      return question_obj

    def get_all( self ):
      """return all objects in an array"""
      """if no user objects are found, returned array should be empty"""
      all_rows = []
      for question in list(self.db.question.find()):
          question_obj = qanda.Question(question["_id"], question["body"])
          all_rows.append(question_obj)
      return all_rows

    def delete( self, id ):
      """delete object with matching id"""
      """KeyError exception should be thrown if id not found"""
      check = self.db.question.count({"_id": id})
      if check is not 0:
          if id is 0:
              raise KeyError
          self.db.question.delete_one({"_id": id})
      else:
          raise KeyError
      return

    def rank( self, offset = 0, limit = 10 ):
      """return entity ids in order of their ranking"""
      """offset limits the return to start at the offset-th rank"""
      """limit parameter limits the maximum number of user ids to be returned"""
      result = []

      pipeline = [
        {"$lookup": {"from": "answers", "foreignField": "qid",
                    "localField": "_id", "as": "order"}},
        {"$project": {"order": {"$size": "$order"}}},
        {"$sort": {"order": -1}},
        {"$skip": offset},
        {"$limit": limit}
      ]

      r = self.db.question.aggregate(pipeline)
      for question in r:
          result.append(qanda.Rank(question["_id"], question["order"]))
      return result

    def new( self, user_id, text ):
      """allow a user to pose a question"""
      """unique question id is returned"""
      """KeyError exception should be thrown if user_id not found"""
      if user_id is not '0':
          qid = str(uuid.uuid4())
          self.db.question.insert_one({"_id": qid,
                                        "uid": user_id,
                                        "body": text})
      return qid
