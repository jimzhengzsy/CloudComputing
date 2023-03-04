# GAS Framework
An enhanced web framework (based on [Flask](https://flask.palletsprojects.com/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](https://getbootstrap.com/docs/3.3/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files


# A14 write-up
For A14-Data-Archive, I use step function to implement waiting 300 seconds and a SNS publish to the data archive topic, which will not blocking any
other functionalities and make the system scalable.
Step function URL:
https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/statemachines/view/arn:aws:states:us-east-1:127134666975:stateMachine:songyuanzheng_a14_archive

In run.py when the job annotation is finished, the step function starts. In archive_script.py, to handle the corner case that free users may decide to upgrade during the 300 seconds,
there is a if statement to check the role of the user to keep the consistency.


# A16 write-up

A16 is iterate from A14. 
For views.py, the subscribe endpoint first, update the role in the profile and session. Query all jobs for that user from dynamodb. For each job, it will extract 'results_file_archive_id' and publish a message to SNS_thaw_topic. 

For thaw_script.py, it keep polling messages from SNS_thaw_topics and handle messages from thaw_queue. The function handle_thaw_queue read message from the queue and process messages, if the archive_id and job_id in the data it will call retrieve_obj to retrieve jobs from glacier.  I will first try expedited retrieval, if it failed, I will try standard retrieval instead. Once the retrieval job finished, it will send notification to job_restore_topic. For the design perspective, I use SNS_thaw_topics to decoupling the annotator and the subscribe process. Once subscribe endpoint is visited, the users role become premium, the process of thawing and restoring starts.


For restore.py, it is a the code of lambda function. The lambda function will check the status of the job. If the job is completed and successful, I will extract the job_id and archive_id to get_job_output from the glacier. After get data from glacier, I will upload data to s3. By using annotation_job_id as the key, I query the job info from dynamodb. From job info I extract bucket name and the key. By using these info, I upload the data to s3. Once the restore complete, the file will be deleted from glacier, the result_file_archive_id of this job in DynamoDB. For the design perspective, the SNS restore topic to decouple the process of thaw and resotre. Restore procedure only starts after the archive-retrieval job complete successfully.



