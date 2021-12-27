# Implementing table partitioning in Oracle Standard Edition: Part 2
This repository is associated with the AWS Databases blog post titled "Implementing table partitioning in Oracle Standard Edition: Part 2". This post is a continuation Implementing table partitioning in Oracle Standard Edition: Part 1, which explains how to configure partitioning in Oracle SE. This post demonstrates techniques of automating date range partition management in Oracle SE.

Check out the blog post for details about the functionality, the setup instructions, and demo use case.

# Demo Instructions 
## Configure partitioning environment and packages (*Listed in partition_steps.txt file*) 
* Create the partition service account user
* Create the main partition table, primary key, and index
* Create the metadata tables to manage partitioning
* 	@partition_mgmt_tables.sql
* 	@pkg_MANAGE_PARTITIONS.sql
* 	@pkg_body_MANAGE_PARTITIONS

## Demo Use Case instruction 
* Daily :- Daily_Test_Scripts.txt
* Weekly:- Weekly_Test_Scripts.txt
* Monthly:-Monthly_Test_Scripts.txt
* Yearly:-Yearly_Test_Scripts.txt

## Auto partioning management
* SQL> EXEC MANAGE_PARTITIONS.MAIN_PROCESS

## Security
See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License
This library is licensed under the MIT-0 License. See the LICENSE file.

