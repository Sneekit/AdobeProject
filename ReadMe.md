### File Information
ProcessFile.py is the main program. It takes an S3 file location as an argument.

Helpers.py contains helping classes for ProcessFile.py

The AWS directory contains IAM policies and Lambda code

The tools directory contains a couple of small tools I wrote for testing

### Challenges Identified
Search engine information leading to actualized revenue was in a separate hit record than the actualized revenue.

Based on the domain of the referrer, the format of the keywords changes. I checked ask.com, duckduckgo, baidu, and yandex to make sure their formats were included.

Lambda functions have a max timeout of 15 minutes. In very large files this may not be enough to process the file, which is why I opted for an S3 -> Lambda -> EC2 -> S3 pipeline.

Lambda functions also have a storage limit of 512MB, while an EFS can be mounted to avoid this, due to the limitation above I opted to forgo serverless processing.

### Assumptions
As the input file is hit data, it was assumed that it was in chronological order.

The client was interested in revenue generated by outside search engines, so any searches within the esshopzilla domain were ignored.

Search engines outside of Google, Yahoo, and Bing were categorized as Other.

### Security
If this was a live production environment, the Lambda should be set up on the same VPC as the EC2, and a VPC endpoint should be configured for the relevant S3 bucket.

Access to the EC2 should only be allowed from within the VPC.

The directory that stores the EC2 key should be it's own bucket with limited access based on IAM.

### Notes
The goal I set for myself was to be able to process files of 10GB or larger with as little RAM utilization as possible.

To track the search engine that led to actualized revenue, a dictionary was used based on ip address. There are two options that I identified to connect search engines to actualized revenue. The first option is to iterate through the file locating actualized revenue and storing the associated ip address in a dictionary, and then iterate through a second time to store search engine information only on these ip addresses. The second option is to store search engine information for every ip address in a dictionary, and reference that when actualized revenue is detected. Option one sacrifices speed for lower RAM utilization, option two is the inverse. I opted for option 2 to get faster results, however it is less scalable.

I summed up the groups as I iterated through the input file as the relevant grouping information was available at the time of parsing. This places the hashmap of groups and their revenue in memory. To scale up, the grouping and sorting could be handled by something that has parallelism ( pyspark/EMR ).

It may be worth noting to the client that an ip address isn't always an accurate representation of a unique visitor ( VPNs etc )

The file format of YYYY_MM_DD restricts the client to one file per day. I would recommend having a time stamp on the file as well so multiple files could be processed in a day.

I moved files from the inbound s3 directory to a processed s3 directory when the processing completes, that way the cron lambda can scan the inbound folder and detect files older than 4 hours and attempt to resubmit them.

If the client only needs to process a single file per day, the Lambda could start up the EC2, and the EC2 could stop itself when it finishes processing.