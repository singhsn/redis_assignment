import redis
import csv
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query

class Redis_Client():
    def __init__(self):
        self.redis = None

    def connect(self):
        try:
            self.redis = redis.Redis(host='localhost', port=6379, db=0)
            print("Connected to Redis.")
        except Exception as e:
            print("Connection Error:", e)

    def load_users(self, file='users.txt'):
        with open(file, 'r') as f:
            count = 0
            for line in f:
                parts = line.strip().split('" "')
                parts = [p.strip('"') for p in parts]
                user_id = parts[0].replace("user:", "")
                user_key = f"user:{user_id}"
                user_data = dict(zip(parts[1::2], parts[2::2]))
                self.redis.hset(user_key, mapping=user_data)
                count += 1
        print(f"{count} users loaded into Redis.")
        return count


    def load_scores(self, file='userscores.csv'):
        with open(file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  
            for row in reader:
                leaderboard = f"leaderboard:{row[2]}"
                self.redis.zadd(leaderboard, {row[0]: float(row[1])})
        print("Scores loaded successfully.")

    def query1(self, usr):
        key = f"user:{usr}"
        result = self.redis.hgetall(key)
        print(f"User {usr} data: {result}")
        return result

    def query2(self, usr):
        key = f"user:{usr}"
        long = self.redis.hget(key, "longitude")
        lat = self.redis.hget(key, "latitude")
        print(f"Coordinates for {usr}: ({long}, {lat})")
        return long, lat

    def query3(self):
        cursor = 1280
        result_ids = []
        last_names = []
        while True:
            cursor, keys = self.redis.scan(cursor=cursor, count=10)
            for key in keys:
                key_str = key.decode()
                if key_str.startswith("user:"):
                    id_str = key_str.split(":")[1]
                    if id_str.isdigit() and int(id_str[0]) % 2 == 0:
                        last_name = self.redis.hget(key_str, "last_name")
                        result_ids.append(key_str)
                        last_names.append(last_name.decode() if last_name else "NA")
            if cursor == 0:
                break
        print("Filtered keys and last names:", list(zip(result_ids, last_names)))
        return result_ids, last_names

    def query4(self):
        # Create index if not exists
        try:
            self.redis.ft().create_index([
                TextField("gender"),
                TagField("country"),
                NumericField("latitude"),
                TextField("first_name")
            ], definition=IndexDefinition(prefix=["user:"]))
        except:
            pass  # Index may already exist

        q = Query("@gender:female @country:{China|Russia} @latitude:[40 46]")
        result = self.redis.ft().search(q)
        for doc in result.docs:
            print(doc)
        return result.docs

    def query5(self):
        print("Executing query 5.")
        try:
            top_users = self.redis.zrevrange("leaderboard:2", 0, 9, withscores=True)

            result = []
            print("Top 10 players in leaderboard:2:")

            for user_id, score in top_users:
                email = self.redis.hget(user_id, 'email')
                if email:
                    result.append(email)
                    print(f"  {user_id}: {email} (score: {score})")

            return result

        except Exception as e:
            print(f"Error in query: {e}")
            return []

# Run
if __name__ == "__main__":
    rs = Redis_Client()
    rs.connect()
    # rs.load_users("users.txt")
    # rs.load_scores("userscores.csv")
    # rs.query1(299)
    # rs.query2(2836)
    # rs.query3()
    # rs.query4()
    rs.query5()
