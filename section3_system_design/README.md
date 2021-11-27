## System Design

### Overview of the files: 
<!-- toc -->
- `system_design_aws.pdf`: AWS infrastructure diagram PDF
- `system_design_aws.png`: AWS infrastructure diagram image
<!-- tocstop -->

### AWS Data Infrastructure for low-latency large scale image processing, analytics and storage
![Model](system_design_aws.png)

he company also has a separate web application which provides . The company’s software engineers have already some code written to process the images. The company would like to save processed images for a minimum of 7 days for archival purposes. Ideally, the company would also want to be able to have some Business Intelligence (BI) on key statistics including number and type of images processed, and by which customers.

Produce a system architecture diagram (e.g. Visio, Powerpoint) using any of the commercial cloud providers' ecosystem to explain your design. Please also indicate clearly if you have made any assumptions at any point.

### Services
-  Company's `Virtual Private Network` (VPC): virtual network for running instances
- `Amazon Kinesis Data Stream`: low latency streaming ingestion of the image data from web application and stream of images
- `Amazon Kinesis Data Firehose`: loads streams into S3 Data after applying processing through AWS Lambda
- `Lambda Function` (Serverless): runs company's provided code for image processing 
- `Amazon Kinesis Data Analytics`: perform real-time analytics on processed image data using SQL and references other data such as customer records
- `S3 Bucket`: 
        1) For storing processed images
        2) For recoring failure cases such as error during processing of an image
        3) For storing some reference data such as customers details 
   Security with IAM policies for role based user access. 
- `Amazon Glacier`: For archiving the processed image data

### Assumptions 
- Images can be upto 1MB in size. Kinesis Data Streams can handle records upto 1MB
- Company has some customer data stored in S3 bucker (see reference data in the diagram) for querying
- For security the company has role-based access to the stored data

### Extra insights
- `Amazon Glue Crawler`, `Amazon Athena` and `Amazon Quicksight` can be used for analytics and visualizations on a dashboard for Business Intelligence.
- Images can be encrypted during transit (by using TLS / SSL) and at rest (by using SSE-S3 and SSE-KMS) for privacy
