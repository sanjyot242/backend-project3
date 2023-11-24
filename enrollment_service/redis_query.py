from redis import Redis
def get_waitlist_count(student_id:int, redis_client: Redis):
    key = f'student_id:{student_id}'
    waitlist_count = str(redis_client.hget(key, "waitlist_count"))
    if waitlist_count is None or waitlist_count == 'None':
        return 0
    return int(waitlist_count)

def increment_wailist_count(student_id:int, redis_client: Redis):
  key = f'student_id:{student_id}'  
  waitlist_count = 0
  current_waitlist_count =  str(redis_client.hget(key, "waitlist_count"))
  if current_waitlist_count is not None and current_waitlist_count != 'None':
    current_waitlist_count = int(current_waitlist_count) + 1
    redis_client.hset(key, mapping={'waitlist_count': current_waitlist_count})
  else:
    redis_client.hset(key, mapping={'waitlist_count': waitlist_count + 1})

def decrement_wailist_count(student_id:int, redis_client: Redis):
  key = f'student_id:{student_id}'  
  current_waitlist_count =  str(redis_client.hget(key, "waitlist_count"))
  current_waitlist_count = int(current_waitlist_count) - 1
  redis_client.hset(key, mapping={'waitlist_count': current_waitlist_count})
