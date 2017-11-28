import json, qanda, uuid, pprint

from pymongo import MongoClient

class AnswerEntity_Impl(qanda.AnswerEntity):


    def __init__(self, db):
        self.db = db

    def initialize( self ):
      self.db.answers.delete_many({"_id": {"$exists": True}})
      return

    def get( self, id ):
      """return object with matching id"""
      """KeyError exception should be thrown if id not found"""
      check = self.db.answer.count({"_id": id})
      if check is not 0:
          row = self.db.answer.find({"qid": id})
      else:
          raise KeyError
      return row

    def get_all( self ):
      """return all objects in an array"""
      """if no user objects are found, returned array should be empty"""
      all_rows = []
      for answer in list(self.db.answers.find()):
          answer_obj = qanda.Answer(answer["_id"], answer["body"], self.VotesCount(answer["_id"], 1), self.VotesCount(answer["_id"], -1))
          all_rows.append(answer_obj)
      return all_rows

    def delete( self, id ):
      """delete object with matching id"""
      """KeyError exception should be thrown if id not found"""
      if id is not '0':
          self.db.answer.delete_one({"_id": id})
      else:
          raise KeyError
      return

    def rank( self, offset = 0, limit = 10 ):
      """return entity ids in order of their ranking"""
      """offset limits the return to start at the offset-th rank"""
      """limit parameter limits the maximum number of user ids to be returned"""
      all_aid_count = []
      pipeline = [
        {"$lookup": {"from": "vote", "foreignField": "v_aid",
                        "localField": "_id", "as": "vote1"}},
        {"$unwind" : {"path" :"$vote1", "preserveNullAndEmptyArrays":True}},
        {"$group": {"_id": "$_id", "total": {"$sum": "$vote1.v_vote"}}},
        {"$sort": {"total": -1}},
        {"$skip": offset},
        {"$limit": limit}
      ]

      r = self.db.answers.aggregate(pipeline)

      for aids in r:
          aid_obj = qanda.Rank(aids["_id"], aids["total"])
          all_aid_count.append(aid_obj)
      return all_aid_count

    def new( self, user_id, question_id, text ):
      """allow a user to answer a question"""
      """unique answer id is returned"""
      """KeyError exception should be thrown if user_id or question_id not found"""
      if user_id is not '0' and question_id is not '0':
          aid = str(uuid.uuid4())

          self.db.answers.insert_one({"_id": aid,
                                        "qid": question_id,
                                        "uid": user_id,
                                        "body": text})
      else:
          raise KeyError
      return aid

    def get_answers( self, question_id ):
      """find all answers to a question"""
      """answers are returned as an array of Answer objects"""
      """KeyError exception should be thrown if question_id not found"""
      answer_arr = []
      for answer in list(self.db.question.find({"answers.qid": question_id })):
          answer_obj = qanda.Answer(answer["_id"], answer["body"], self.VotesCount(answer["_id"], 1), self.VotesCount(answer["_id"], -1))
          answer_arr.append(obj)
      return answer_arr

    def VotesCount(self, aid, val):
      result = self.db.vote.aggregate([
      {"$match":{"v_aid":aid, "v_vote":val}},
      {"$group":{"_id":"_id", "count":{"$sum":1}}}
      ])
      for question in result:
        return question["count"]
      return 0

    def vote( self, user_id, answer_id, vote ):
      """allow a user to vote on a question; vote is of class Vote"""
      """up and down votes are returned as a pair"""
      """KeyError exception should be thrown if user_id or answer_id not found"""
      if not self.db.user.find_one({"_id" : user_id}):
          raise KeyError
      ch = self.db.answers.find_one({"_id" : answer_id})
      if not self.db.answers.find_one({"_id" : answer_id}):
          raise KeyError
      self.db.vote.insert_one({"v_uid": user_id,
                               "v_aid": answer_id,
                               "v_vote": vote.value})
