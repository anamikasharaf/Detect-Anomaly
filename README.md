# Detect-Anomaly
Big Data MapReduce

README

Detect Anomalous Failed Login Behavior

Given an audit log with multiple types of records, we are interested in those records of type USER_LOGIN (the rest you should discard). 
The output of the program should be the list of users whose anomaly score is higher than the threshold specified by the user at the command line.

1. MapReduce Program 1

The first MapReduce program (MRdriver.java, MRmapper1.java, and MRreducer1.java) will calculate the following statistic:
	•	failed_login_attempts_for_acct

Note: acct must be replaced by each user name as defined in the "acct" field of the USER_LOGIN record. In other words, there will be multiple output lines for the failed_login_attempts_for_acct statistic (one per user name with failed logins).

2. MapReduce Program 2

The second MapReduce program (MRdriver.java, MRmapper2.java, and MRreducer2.java)  will calculate the following statistics:
	•	mean_failed_login_attempts
	•	sigma_failed_login_attempts
	•	num_sigmas_for:acct
 
Note: acct must be replaced by each user name as defined in the "acct" field of the USER_LOGIN record. In other words, there will be multiple output lines for the num_sigmas_for:acct statistic (one per user name with failed logins).

3. Output

Output should be something like this:
Detected Anomaly for user: num_sigmas_for: “abc”  with score: 3.456


