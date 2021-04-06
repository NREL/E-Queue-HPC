import uuid
import jobqueue

def test_interface():

    jq = jobqueue.JobQueue("test", 'test_queue')
    
    # 1. Fill the queue up
    # clear the queue
    jq.clear()
    assert jq.messages == 0

    # add jobs to queue.  The job does not make the record unique.  You can
    # pass a uuid if you'd like.
    jq.add_job({'job': 1, 'uuid': str(uuid.uuid4()), 'a_list': [1,2,4]})
    jq.add_job({'job': 1, 'a_list': [1,2,4]})
    jq.add_job({'job': 1, 'a_list': [1,2,4]}, priority=1)
    assert jq.messages == 3

    # 2. Run jobs
    # now request a message
    message = jq.get_message()
    assert jq.messages == 2


    print(message.uuid)
    print(message.priority)
    print(message.config)
    
    # mark job as finished
    message.mark_complete()







    

