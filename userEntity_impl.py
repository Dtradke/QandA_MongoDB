import json, qanda, uuid, pprint
from pymongo import MongoClient

#all tests pass! Test 5 takes a while though

def rsscore(self):
    if self.score is None:
        return 0
    return self.score

class UserEntity_Impl(qanda.UserEntity):

    def __init__(self, db):
        self.db = db

    def initialize( self ):
      self.db.user.delete_many({"_id": {"$exists": True}})
      return

    def get( self, id ):
      """return object with matching id"""
      """KeyError exception should be thrown if id not found"""
      for user in list(self.db.user.find({"_id": id})):
        user_obj = qanda.User(user["_id"],user["email"],user["passhash"])
      return user_obj

    def get_all( self ):
      """return all objects in an array"""
      """if no user objects are found, returned array should be empty"""
      all_rows = []
      for user in list(self.db.user.find()):
          obj = qanda.User(user["_id"],user["email"],user["passhash"])
          all_rows.append(obj)
      return all_rows

    def delete( self, id ):
      """delete object with matching id"""
      """KeyError exception should be thrown if id not found"""
      check = self.db.user.count({"_id": id})
      if check is not 0:
          self.db.user.delete_one({"_id": id})
      else:
          raise KeyError
      return

    def rank( self, offset = 0, limit = 10 ):
      """return entity ids in order of their ranking"""
      """offset limits the return to start at the offset-th rank"""
      """limit parameter limits the maximum number of user ids to be returned"""
      result = []
      rs = dict()
      r = self.db.user.aggregate([{"$project":{"_id":1}},{"$skip": offset},{"$limit": limit}])
      for q in r:
          rs.update({str(q["_id"]): qanda.Rank(q["_id"], None)})
      pipeline = [
        {"$lookup": {"from": "answers", "foreignField": "uid",
                        "localField": "_id", "as": "answer"}},
        {"$unwind" : {"path" :"$answer"}},
        {"$lookup": {"from": "question", "foreignField": "_id",
                        "localField": "answer.qid", "as": "quest"}},
        {"$unwind" : {"path": "$quest"}},
        {"$match" : {"quest.uid:0" : {"$ne" : "_id"}}},
        {"$group" : {"_id": "$_id", "count" : {"$sum" : 1}}},
        {"$sort": {"count": -1}},
        {"$skip": offset},
        {"$limit": limit}
      ]
      r = self.db.user.aggregate(pipeline)
      for qu in r:
          rs[str(qu["_id"])].score = qu["count"]
      #if result == []:
    #    result = [ qanda.Rank(None, None) ]
      return sorted(rs.values(), key = rsscore, reverse = True)
      return []

    def new( self, email, passhash = None ):
      """create a new instance in db using the given parameters"""
      """unique user id is returned"""
      """if email already exists, KeyError exception will be thrown"""
      uid = str(uuid.uuid4())
      self.db.user.insert_one({"_id": uid,
                                "email": email,
                                "passhash": passhash})
      return uid
