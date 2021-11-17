
![](jpg/asset_abstract_9.jpg)

# The Snowflake/AWS Autopilot Integration: Machine Learning in SQL

Taking advantage of ML technology usually requires a lot of infrastructure, a multitude of software packages and a small army of highly-skilled engineers building, configuring, and maintaining the complex environment. But what if you could take advantage of machine learning capabilities using SQL from end-to-end? Building a model and scoring datasets at scale without having to learn a new language, without having to provision and manage infrastructure, and without the overhead of maintaining additional software packages. How could that impact the bottom line of your business? The Snowflake/AWS Autopilot integration is exactly that. It combines the power of Snowflake to process data at near infinite scalability with the managed AutoML feature in AWS called Autopilot.

In this blog post we will explore an end-to-end example of building a scalable process from data ingestion to scoring millions of data points in seconds using nothing but SQL. And even though the setup might look a bit complicated, it is completely script driven and only takes 3 simple steps. If you are only interested in the ML part of the post, just skip over the setup steps and start with section “Snowflake/Autopilot Integration”.

## Prerequisites

You need access to an AWS and a Snowflake account. If you do not already have access, follow the links for a [free AWS](https://aws.amazon.com/free/) and a [free Snowflake](https://signup.snowflake.com/) account.

Next, clone the project's github repo. It includes all artifacts needed to create the AWS and Snowflake resources as well as the dataset we are going to analyze.

```
git clone --recurse-submodules https://github.com/Snowflake-Labs/sfguide-aws-autopilot-integration.git
```

Note: The recurse-submodules flag is required because the repo references a submodule from the AWS-Samples repo.

## Use Case

The dataset we will explore is the [Synthetic Financial Datasets For Fraud Detection](https://www.kaggle.com/ealaxi/paysim1) on [kaggle](https://www.kaggle.com).

The dataset represents a synthetic set of credit card transactions. Some of those transactions have been labeled as fraudulent, but most of them are not fraudulent. In fact, if you review the documentation on Kaggle, you will find that 99.9% of the transactions are non-fraudulent.

The goal of this exercise is to build an ML model that accurately predicts both transaction types, i.e. fraudulent as well as non-fraudulent transactions. After all, who wants to be sitting in a restaurant after a fantastic dinner and be totally embarrassed by their credit card being declined because the credit card company's transaction model hit a false positive and declined your transaction.

## Setup

Building a hands-on environment that will allow you to build and score the Model in your environment is very straightforward. It requires 3 simple, script-driven steps.

1. Snowflake configuration: Run script setup.sql from your Snowflake console.
1. Credentials configuration: Configure credentials in AWS Secrets Manager from the AWS console.
1. Integration configuration: Run the CouldFormation script from the AWS console.

Pro tip: Alternatively,you could run all steps from CLI commands.

### Snowflake configuration

The Snowflake setup is completely-script driven. The  [script](scripts/setup.sql) creates a login, which we will use later to perform all steps executed in Snowflake, and a database and schema which holds all objects, i.e. tables, external functions, and JavaScript functions. Please note, that by creating a new user (and role), we don’t have to use ACCOUNTADMIN to run all subsequent steps in this demo. Be sure to update the password, first name, last name, and email address before you run it.

### Credentials configuration

The instructions to build all other resources, i.e. API Gateway, S3 bucket, and all Snowflake external and JavaScript functions will be created via an AWS Cloud formation script. Sensitive information like login, password, and fully qualified account ID will be stored using the AWS Secrets Manager.

To get started, log into your AWS account and search for Secrets Manager in the Search box in the AWS Console.

In the Secrets Manager UI, create the three key/value pairs below. Be sure to configure the fully qualified account ID (including region and cloud).

<p align="center"><img src="jpg/secrets_conf.png" width="300" height="300" /></p>

Give your secrets configuration a name and save it.

Next, find your secrets configuration again (the easiest way is to search for it via the Search input field), and copy the Secret ARN. We will need it in the next step when configuring the CloudFormation script.

### Integration Configuration (CloudFormation)

The last step is to configure the Snowflake/Autopilot Integration. This used to be a very time-consuming and error-prone process but with AWS CloudFormation it’s a “piece of cake”.

All necessary permissions required to run this CloudFormation script are included in the policies.zip file in the repo. However, for the purpose of this demo we are assuming that you have root access to the AWS console.

Start with selecting the CloudFormation service from the AWS console.

Then, click the “Create Stack” button at the top right corner.

“Template is ready” should be selected by default. Click “Upload a template file” and select the template file “customer-stack.yml” from the repo at “amazon-sagemaker-integration-with-snowflake/customer-stack.

The next screen allows you to enter the stack details for your environment. These are:

- Stack name
- apiGatewayName
- s3BucketName
- database name and schema name
- role to be used for creating the Snowflake objects (external functions and JS function)
- Secrets ARN (from above)

Be sure to pick consistent names because the AWS resources must be unique in your environment.

<p align="center"><img src="jpg/cloudformation.png" width="300" height="300" /></p>

Go with the defaults on the next two screens, so click “Next” twice.

Click the “Acknowledge” checkbox and continue with “Create Stack”.

You can follow the “stack creation” by clicking the “Refresh” button. Eventually, you should see “CREATE_COMPLETE”.

Creating the stack should take about one minute. At this point, the integration has been completely set up and we can head over to Snowflake to start with the data engineering steps.

## Snowflake/Autotpilot Integration

To follow best practices, we will not use the ACCOUNTADMIN role to run the steps for this demo. Therefore, log in into Snowflake with user “autopilot_user”. The password should be in your setup scripts.

The demo consists of 4 major steps and all steps will be initiated via [SQL commands](scripts/demo.sql).

1. Data engineering (import dataset and simple data prep)
1. Build initial model
1. Score test dataset and evaluate model
1. Optimize model (including hyperparameter tuning)


### Data Engineering

To make it easier to import the dataset into your Snowflake instance, (the dataset is stored as a zip file on Kaggle), I have included the dataset in the github repo, split into 4 gzipped files. Importing the dataset directly into a Snowflake table is very simple. But before we can load the dataset, we first have to create a table and define a file format to use during the loading process.

```
use role autopilot_role;
use database autopilot_db;
use warehouse autopilot_wh;
create or replace table cc_dataset (
 step number(38,0),
 type varchar(16777216),
 amount number(38,2),
 nameorig varchar(16777216),
 oldbalanceorg number(38,2),
 newbalanceorig number(38,2),
 namedest varchar(16777216),
 oldbalancedest number(38,2),
 newbalancedest number(38,2),
 isfraud boolean,
 isflaggedfraud boolean
) ;
create file format cc_file_formaat type=csv  skip_header=1;
```

Next, head over to the database tab and click Autopilot_db, then click the table we want to load, i.e. cc_dataset, and click “Load Table”.

Follow the steps in the load wizard, and select all 4 files from the data directory in the repo.

Confirm the file format (it should already be pre-selected) and start the loading process. The whole process takes about 5 minutes. ncrypting the dataset taking the majority of the time (over 4 minutes).

Pro tip: Instead of using the Snowflake Web UI for loading the dataset into a table, you can upload the files first via the put command in snowsql. Then, load the files using command line “copy into” statements. This method is more scalable and much faster (30 secs vs 300 secs).

Let’s briefly review the dataset.

```
select * from cc_dataset;
```

The dataset includes  about 6.3 million credit card transaction and has a variety of different attributes.The attribute we want to predict is called "isfraud" of datatype boolean. 

Let’s review the data distribution of the “isfraud” attribute.

```
select isfraud, count(*) from cc_dataset group by isfraud;
```

As you can see, there is a massive class imbalance in the “isfraud” attribute. In this dataset we have 99.9% non-fraudulent transactions and a very small number of transactions have been classified as fraudulent.

Usually the next step would be data preparation and feature engineering. For this demo we will skip this step. If you want to learn more about data preparation and feature generation, please refer to the links at the end of this post.

The only step left in terms of data engineering is to split the dataset into a training and a test dataset. For this demo we will go with a 50/50 split. In Snowflake SQL, this can be accomplished very easily with these two statements:

```
create or replace table cc_dataset_train as
   select * from cc_dataset sample (50);
create or replace table cc_dataset_test as
   (select * from cc_dataset ) minus 
   (select * from cc_dataset_train) ;
```

For good measure, let’s check the split and that we have a reasonable number of each class value in our test and training tables.

```
(select 'cc_dataset_train' name, isfraud, count(*) 
 from cc_dataset_train group by isfraud) 
union
(select 'cc_dataset_test' name, isfraud, count(*) 
 from cc_dataset_test group by isfraud)
```

As you can see (your numbers might be slightly different), we have an almost perfect 50/50 split with nearly 50% of the fraud cases in either dataset. Of course, in a real world usecase we would take a much closer look at all attributes to ensure that we haven’t introduced bias unintentionally.

### Building the Model

This is where the “rubber meets the road” and where we would usually switch to a different programming environment like Python or Scala, and use different ML packages, like Scikit-Learn, PyTorch, TensorFlow, MLlib, H20 Sparkling Water, the list goes on and on. However, with the Snowflake integration to AWS Autopilot we can initiate the model building process directly from within your Snowflake session using regular SQL syntax and AWS Autopilot does the rest.

```
select aws_autopilot_create_model (
  'cc-fraud-prediction'  -- model name
  ,'cc_dataset_train'    -- training data location
  ,'isfraud'             -- target column
  ,null                  -- objective metric
  ,null                  -- problem type
  ,5                     -- number of candidates to be evaluated
                         --    via hyperparameter tuning 
  ,15*60*60              -- training timeout
  ,'True'                -- create scoring endpoint yes/no
  ,1*60*60               -- endpoint TTL
);
```

Let’s review the SQL statement above. It calls a function that accepts a few parameters. Without going into too much detail (most parameters are pretty self-explanatory), here are the important ones to note:

- Model Name: This is the name of the model to be created. The name must be unique. Models are read-only and they can not be updated. There is no programmatic way to delete a model If you want to rebuild a model, append a sequential number to the base name to keep the name unique.
- Training Data Location: This is the name of the table storing the data used to train the model.
- Target Column: This is the name of the column in the training table we want to predict.


To check the current status of the model build process we call another function in the Snowflake/AWS Autopilot integration package.

```
select aws_autopilot_describe_model('cc-fraud-prediction');
```

When you call the aws_autopilot_describe_model() function repeatedly you will find that the model build process goes through several state transitions.

Building the model should take about 1 hour and eventually you should see “JobStatus=Completed” when you call the aws_autopilot_describe_model() function.

And that’s it. That’s all we had to do to build a model from an arbitrary dataset. Just pick your dataset to train the model with, the attribute you want to predict, and start the process. Everything else, like building the infrastructure needed to train the model, picking the right algorithm for training the model, and tuning hyperparameters for optimizing the accuracy are all done automatically.

### Testing the Model

Now, let’s check how well our model achieves the goal of predicting fraud. For that, we need to score the test dataset. The scoring function takes 2 parameters:

- Endpoint Name: This is the name of the API endpoint. The model training process has a parameter controlling whether or not an endpoint is created and if so, what its TTL (time to live) is. By default the endpoint name is the same name as the model name.
- Attributes: This is an array of all attributes used during the model training process.

```
create or replace table cc_dataset_prediction_result as 
  select isfraud,(parse_json(
      aws_autopilot_predict_outcome(
        'cc-fraud-prediction'
        ,array_construct(
           step,type,amount,nameorig,oldbalanceorg,newbalanceorig
           ,namedest,oldbalancedest,newbalancedest,isflaggedfraud))
    ):"predicted_label")::varchar predicted_label
  from cc_dataset_train;
```

If you get an error message saying “Could not find endpoint” when calling the aws_autopilot_predict_outcome() function, it might mean that even though the endpoint had been created during the model training process, it has expired.

To check the endpoint, call aws_autopilot_describe_endpoint(). You will get an error message if the endpoint doesn’t exist.

```
select aws_autopilot_describe_endpoint('cc-fraud-prediction');
```

To restart the endpoint call the function aws_autopilot_create_endpoint(), which takes 3 parameters.

- Endpoint Name: By default, the function aws_autopilot_create_endpoint() creates an endpoint with the same name as the model name. But you can use any name you like, for instance to create a different endpoint for a different purpose, like development or production.
- Endpoint configuration name: By default, the function aws_autopilot_create_endpoint() creates an endpoint configuration named "model_name"-m5–4xl-2. This name follows a naming convention like "model name"-"instance type"-"number of instances". This means that the default endpoint is made up of two m5.4xlarge EC2 instances.
- TTL: TTL means “time to live”. This is the amount of time the endpoint will be active. For TTL, it does not matter whether or not the endpoint is used. If you know that you no longer need the endpoint, it is cost effective to delete the endpoint by calling aws_autopilot_delete_endpoint(). Remember, if necessary, you can always re-create the endpoint by calling aws_autopilot_create_endpoint().


```
select aws_autopilot_create_endpoint (
    'cc-fraud-prediction' 
    ,'cc-fraud-prediction-m5-4xl-2' 
    ,1*60*60);
```

This is an asynchronous function, meaning it completes immediately but we have to check with function aws_autopilot_descrive_endpoint() until the endpoint is ready.

After having validated that the endpoint is running, and scoring the test dataset with the statement above, we are ready to compute the accuracy of our model. To do so we count all occurrences for each of the 4 combinations between the actual and the predicted value. An easy way to do that is to use an aggregation query grouping by those 2 attributes.

```
select isfraud, predicted_label, count(*)
from cc_dataset_prediction_result
group by isfraud, predicted_label
order by isfraud, predicted_label;
```

To compute the overall accuracy, we then add up the correctly predicted values and divide by the total number of observations. Your numbers might be slightly different but it will be in the 99% range.

<p align="center"><img src="jpg/initial_all.png" width="300" height="300" /></p>

Pretty good, right? Next, let’s drill down and review the accuracy for each of the predicted classes: not fraudulent (majority class) and fraudulent (minority class).

<p align="center"><img src="jpg/initial_detail.png" width="300" height="300" /></p>

And that’s where our model shows some problems. Although the majority class is in the 99.99% range, the minority class has a very high rate of false positives. This means that if our model predicts a fraudulent transaction it will be wrong 4 times out of 5. In a practical application, this model would be useless.

So what’s the problem? The main reason for this poor performance is that accurately identifying the minority class in a massively imbalanced class distribution is very difficult for ML algorithms. Though it’s not impossible, it requires hundreds of experiments while tuning different parameters.

So what can we do to fix it, you ask? That’s where AutoML systems like Autopilot really shine. Instead of having to manually modify the different parameters, Autopilot will automatically pick reasonable values for each parameter, combine them with parameter sets, compute a model for each parameter set, and evaluate the accuracy. Finally, Autopilot will pick the best model based on accuracy, and will build an endpoint that is ready to use.

### Optimize the Model

To get a much more accurate model, we can use the defaults for the function aws_autopilot_create_model(). Instead of supplying 9 parameters, we only supply the first three parameters. The Snowflake integration with Autopilot automatically picks default values for all of the other parameters. The main difference is that the default number of candidates is 250 instead of 5 as configured before.

```
select aws_autopilot_create_model (
  'cc-fraud-prediction-final' -- model name
  ,'cc_dataset_train'         -- training data table name
  ,'isfraud'                  -- target column
);
```

Like you did before, run the scoring function after the model has been built. Check the status periodically using the function aws_autopilot_describe_model(). Re-create the endpoint if it doesn’t exist using the function aws_autopilot_create_endpoint().

Finally, score the test dataset using aws_autopilot_predict_outcome() and route the output into a different results table.

```
create or replace table cc_dataset_prediction_result_final as 
  select isfraud,(parse_json(
      aws_autopilot_predict_outcome(
        'cc-fraud-prediction'
        ,array_construct(
           step,type,amount,nameorig,oldbalanceorg,newbalanceorig
           ,namedest,oldbalancedest,newbalancedest,isflaggedfraud))
    ):"predicted_label")::varchar predicted_label
  from cc_dataset_train;
```

Then count the observations again by actual and predicted value.

```
select isfraud, predicted_label, count(*)
from cc_dataset_prediction_result_final
group by isfraud, predicted_label
order by isfraud, predicted_label;
```

<p align="center"><img src="jpg/final_all.png" width="300" height="300" /></p>

The overall accuracy is still in the upper 99% range and that is good. However, Autopilot shows its real power when we review the per class accuracy. It has improved from 18.5% to a whopping 88.88%.

<p align="center"><img src="jpg/final_detail.png" width="300" height="300" /></p>

With these results, there is just one obvious question left to answer. Why wouldn’t we always let the Snowflake integration pick all parameters? The main reason is the time it takes to create the model with default parameters. In this particular example it takes 9 hours to produce an optimal model. So if you just want to test the end-to-end process, you may want to test by asking Autopilot to create only 1 model. However, when you want to get a model with the best accuracy, go with the defaults.

## Conclusion

Having managed ML capabilities directly in the data cloud provides incredibly exciting new capabilities. It opens up the world of machine learning for data engineers, data analysts, and data scientists who are primarily working in a more SQL-centric environment. Not only can you take advantage of all the benefits of the Snowflake Data Cloud but now you can add full ML capabilities (model building and scoring) from the same interface. As you have seen in this article, AutoML makes ML via SQL extremely powerful and easy to use.
