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