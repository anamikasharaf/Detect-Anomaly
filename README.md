# Detect-Anomaly

Detect Anomalous Failed Login Behavior with MapReduce

# Objective

Given an audit log with multiple types of records, we are interested in those records of type USER_LOGIN. 
The output of the program should be the list of users whose anomaly score is higher than the threshold specified by the user at the command line.

# Data
Here is a sample failed USER_LOGIN record:

type=USER_LOGIN msg=audit(1453738391.690:107584): user pid=23159 uid=0 auid=4294967295 ses=4294967295 msg='op=login acct="simth" exe="/usr/sbin/sshd" hostname=? addr=10.20.30.200 terminal=ssh res=failed' 

NOTE: this is a failed login attempt by a user called "smith"

Here is a sample successful USER_LOGIN record:

type=USER_LOGIN msg=audit(1456937762.214:56406): user pid=6548 uid=0 auid=496 ses=5648 msg='op=login id=496 exe="/usr/sbin/sshd" hostname=172.30.3.169 addr=172.30.3.169 terminal=ssh res=success'

NOTE: this is a successful login attempt by a user with id=496. Also note that the successful USER_LOGIN record contains an id in the msg, NOT an acct.


## 1. MapReduce Program 1

	The first MapReduce program (MRdriver.java, MRmapper1.java, and MRreducer1.java) will calculate the following     	  statistic:
		
		•	failed_login_attempts_for_acct


## 2. MapReduce Program 2

	The second MapReduce program (MRdriver.java, MRmapper2.java, and MRreducer2.java)  will calculate the following 	statistics:
		
		•	mean_failed_login_attempts
		•	sigma_failed_login_attempts
		•	num_sigmas_for:acct
 

## Output

	Output should be:
	
	Detected Anomaly for user: num_sigmas_for: “abc”  with score: 3.456


